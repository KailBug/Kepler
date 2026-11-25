package runtime

import (
	"context"
	"errors"
	"time"

	connect "connectrpc.com/connect/v2"
	keplerv1 "github.com/KailBug/Kepler/gen/go/kepler/v1"
)

const (
	defaultLeaseTTL         = 30 * time.Second
	defaultHeartbeatMs      = 10_000
	defaultPollInterval     = 100 * time.Millisecond
	maxAcquireWaitTimeoutMs = 30_000
)

// WorkerServiceOptions customizes timing and notification hooks for WorkerService.
type WorkerServiceOptions struct {
	LeaseTTL         time.Duration
	HeartbeatMs      int64
	PollInterval     time.Duration
	OnEventsAppended func(sessionID string, lastSeq int64)
}

// WorkerService implements keplerv1connect.WorkerServiceHandler.
type WorkerService struct {
	store            Store
	leaseTTL         time.Duration
	heartbeatMs      int64
	pollInterval     time.Duration
	onEventsAppended func(sessionID string, lastSeq int64)
	taskNotify       chan struct{}
}

func NewWorkerService(store Store, opts WorkerServiceOptions) *WorkerService {
	leaseTTL := opts.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = defaultLeaseTTL
	}
	heartbeatMs := opts.HeartbeatMs
	if heartbeatMs <= 0 {
		heartbeatMs = defaultHeartbeatMs
	}
	pollInterval := opts.PollInterval
	if pollInterval <= 0 {
		pollInterval = defaultPollInterval
	}
	return &WorkerService{
		store:            store,
		leaseTTL:         leaseTTL,
		heartbeatMs:      heartbeatMs,
		pollInterval:     pollInterval,
		onEventsAppended: opts.OnEventsAppended,
		taskNotify:       make(chan struct{}, 1),
	}
}

// NotifyNewTask wakes up a long-polling AcquireTask caller immediately.
func (w *WorkerService) NotifyNewTask() {
	select {
	case w.taskNotify <- struct{}{}:
	default:
	}
}

func (w *WorkerService) RegisterWorker(ctx context.Context, req *keplerv1.RegisterWorkerRequest) (*keplerv1.RegisterWorkerResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "worker_id is required")
	}
	if req.GetRuntime() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "runtime is required")
	}
	if err := w.store.RegisterWorker(ctx, WorkerRegistration{
		WorkerID:     req.GetWorkerId(),
		Runtime:      req.GetRuntime(),
		Version:      req.GetVersion(),
		Capabilities: req.GetCapabilities(),
	}); err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.RegisterWorkerResponse{HeartbeatIntervalMs: w.heartbeatMs}, nil
}

func (w *WorkerService) Heartbeat(ctx context.Context, req *keplerv1.HeartbeatRequest) (*keplerv1.HeartbeatResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "worker_id is required")
	}
	if err := w.store.HeartbeatWorker(ctx, req.GetWorkerId(), req.GetActiveTasks()); err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.HeartbeatResponse{}, nil
}

func (w *WorkerService) AcquireTask(ctx context.Context, req *keplerv1.AcquireTaskRequest) (*keplerv1.AcquireTaskResponse, error) {
	if req.GetWorkerId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "worker_id is required")
	}

	waitMs := req.GetWaitTimeoutMs()
	if waitMs < 0 {
		waitMs = 0
	}
	if waitMs > maxAcquireWaitTimeoutMs {
		waitMs = maxAcquireWaitTimeoutMs
	}
	deadline := time.Now().Add(time.Duration(waitMs) * time.Millisecond)

	for {
		acq, err := w.store.AcquireTask(ctx, req.GetWorkerId(), w.leaseTTL)
		if err != nil {
			return nil, mapStoreError(err)
		}
		if acq != nil {
			pbEvents := make([]*keplerv1.SessionEvent, len(acq.Events))
			for i, ev := range acq.Events {
				pbEv, err := EventToProto(ev)
				if err != nil {
					return nil, connect.NewError(connect.CodeInternal, err.Error())
				}
				pbEvents[i] = pbEv
			}
			return &keplerv1.AcquireTaskResponse{
				Task: &keplerv1.WorkerTask{
					TaskId:      acq.Task.ID,
					SessionId:   acq.Task.SessionID,
					TurnId:      acq.Task.TurnID,
					AgentId:     acq.Task.AgentID,
					Lease:       LeaseToProto(acq.Task.Lease),
					Events:      pbEvents,
					Traceparent: acq.Task.Traceparent,
				},
			}, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return &keplerv1.AcquireTaskResponse{}, nil
		}
		sleep := w.pollInterval
		if remaining < sleep {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, connect.NewError(connect.CodeCanceled, ctx.Err().Error())
		case <-w.taskNotify:
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (w *WorkerService) RenewLease(ctx context.Context, req *keplerv1.RenewLeaseRequest) (*keplerv1.RenewLeaseResponse, error) {
	if err := validateLeaseArgs(req.GetTaskId(), req.GetLeaseId(), req.GetEpoch()); err != nil {
		return nil, err
	}
	lease, err := w.store.RenewLease(ctx, req.GetTaskId(), req.GetLeaseId(), req.GetEpoch(), w.leaseTTL)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.RenewLeaseResponse{Lease: LeaseToProto(lease)}, nil
}

func (w *WorkerService) AppendEvents(ctx context.Context, req *keplerv1.AppendEventsRequest) (*keplerv1.AppendEventsResponse, error) {
	if req.GetSessionId() == "" || req.GetLeaseId() == "" || req.GetEpoch() <= 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id, lease_id, and positive epoch are required")
	}
	events := make([]Event, len(req.GetEvents()))
	for i, pbEv := range req.GetEvents() {
		ev, err := EventFromProto(pbEv)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, err.Error())
		}
		events[i] = ev
	}
	lastSeq, err := w.store.AppendEvents(ctx, req.GetSessionId(), req.GetLeaseId(), req.GetEpoch(), events)
	if err != nil {
		return nil, mapStoreError(err)
	}
	if w.onEventsAppended != nil && len(events) > 0 {
		w.onEventsAppended(req.GetSessionId(), lastSeq)
	}
	return &keplerv1.AppendEventsResponse{LastSeq: lastSeq}, nil
}

func (w *WorkerService) CompleteTask(ctx context.Context, req *keplerv1.CompleteTaskRequest) (*keplerv1.CompleteTaskResponse, error) {
	if err := validateLeaseArgs(req.GetTaskId(), req.GetLeaseId(), req.GetEpoch()); err != nil {
		return nil, err
	}
	if err := w.store.CompleteTask(ctx, req.GetTaskId(), req.GetLeaseId(), req.GetEpoch()); err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.CompleteTaskResponse{}, nil
}

func (w *WorkerService) SuspendTask(ctx context.Context, req *keplerv1.SuspendTaskRequest) (*keplerv1.SuspendTaskResponse, error) {
	if err := validateLeaseArgs(req.GetTaskId(), req.GetLeaseId(), req.GetEpoch()); err != nil {
		return nil, err
	}
	if err := w.store.SuspendTask(ctx, req.GetTaskId(), req.GetLeaseId(), req.GetEpoch(), req.GetReason()); err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.SuspendTaskResponse{}, nil
}

func (w *WorkerService) FailTask(ctx context.Context, req *keplerv1.FailTaskRequest) (*keplerv1.FailTaskResponse, error) {
	if err := validateLeaseArgs(req.GetTaskId(), req.GetLeaseId(), req.GetEpoch()); err != nil {
		return nil, err
	}
	if err := w.store.FailTask(ctx, req.GetTaskId(), req.GetLeaseId(), req.GetEpoch(), req.GetError(), req.GetRetryable()); err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.FailTaskResponse{}, nil
}

func validateLeaseArgs(taskID, leaseID string, epoch int64) error {
	if taskID == "" || leaseID == "" || epoch <= 0 {
		return connect.NewError(connect.CodeInvalidArgument, "task_id, lease_id, and positive epoch are required")
	}
	return nil
}

func mapStoreError(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err.Error())
	case errors.Is(err, ErrStaleEpoch):
		return connect.NewError(connect.CodeFailedPrecondition, err.Error())
	case errors.Is(err, ErrSeqConflict):
		return connect.NewError(connect.CodeAborted, err.Error())
	case errors.Is(err, ErrSessionBusy), errors.Is(err, ErrNotWaitingReview):
		return connect.NewError(connect.CodeFailedPrecondition, err.Error())
	default:
		return connect.NewError(connect.CodeInternal, err.Error())
	}
}
