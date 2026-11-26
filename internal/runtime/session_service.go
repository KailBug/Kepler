package runtime

import (
	"context"
	"strings"

	connect "connectrpc.com/connect/v2"
	keplerv1 "github.com/KailBug/Kepler/gen/go/kepler/v1"
	"github.com/KailBug/Kepler/gen/go/kepler/v1/keplerv1connect"
)

// SessionServiceOptions wires notifications between SessionService, Broadcaster, and WorkerService.
type SessionServiceOptions struct {
	Broadcaster   *Broadcaster
	OnTaskCreated func()
}

// SessionService implements keplerv1connect.SessionServiceHandler.
type SessionService struct {
	store         Store
	broadcaster   *Broadcaster
	onTaskCreated func()
}

func NewSessionService(store Store, opts SessionServiceOptions) *SessionService {
	bc := opts.Broadcaster
	if bc == nil {
		bc = NewBroadcaster()
	}
	return &SessionService{
		store:         store,
		broadcaster:   bc,
		onTaskCreated: opts.OnTaskCreated,
	}
}

// Broadcaster returns the shared broadcaster for REST/SSE handlers.
func (s *SessionService) Broadcaster() *Broadcaster { return s.broadcaster }

func (s *SessionService) notifyTask() {
	if s.onTaskCreated != nil {
		s.onTaskCreated()
	}
}

func (s *SessionService) CreateSession(ctx context.Context, req *keplerv1.CreateSessionRequest) (*keplerv1.CreateSessionResponse, error) {
	sess, err := s.store.CreateSession(ctx, req.GetAgentId())
	if err != nil {
		return nil, mapStoreError(err)
	}
	return &keplerv1.CreateSessionResponse{Session: SessionToProto(sess)}, nil
}

func (s *SessionService) ListSessions(ctx context.Context, req *keplerv1.ListSessionsRequest) (*keplerv1.ListSessionsResponse, error) {
	sessions, err := s.store.ListSessions(ctx, req.GetAgentId(), int(req.GetLimit()))
	if err != nil {
		return nil, mapStoreError(err)
	}
	out := make([]*keplerv1.SessionInfo, len(sessions))
	for i, sess := range sessions {
		out[i] = SessionToProto(sess)
	}
	return &keplerv1.ListSessionsResponse{Sessions: out}, nil
}

func (s *SessionService) GetSession(ctx context.Context, req *keplerv1.GetSessionRequest) (*keplerv1.GetSessionResponse, error) {
	if req.GetSessionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	sess, evs, err := s.store.GetSession(ctx, req.GetSessionId())
	if err != nil {
		return nil, mapStoreError(err)
	}
	pbEvents := make([]*keplerv1.SessionEvent, len(evs))
	for i, ev := range evs {
		pbEv, err := EventToProto(ev)
		if err != nil {
			return nil, connect.NewError(connect.CodeInternal, err.Error())
		}
		pbEvents[i] = pbEv
	}
	return &keplerv1.GetSessionResponse{
		Session: SessionToProto(sess),
		Events:  pbEvents,
	}, nil
}

func (s *SessionService) SendMessage(ctx context.Context, req *keplerv1.SendMessageRequest) (*keplerv1.SendMessageResponse, error) {
	if req.GetSessionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if strings.TrimSpace(req.GetText()) == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "text must be non-empty")
	}
	turnID, _, _, err := s.store.SubmitUserMessage(ctx, req.GetSessionId(), req.GetText())
	if err != nil {
		return nil, mapStoreError(err)
	}
	s.broadcaster.Notify(req.GetSessionId())
	s.notifyTask()
	return &keplerv1.SendMessageResponse{TurnId: turnID}, nil
}

func (s *SessionService) ResolveApproval(ctx context.Context, req *keplerv1.ResolveApprovalRequest) (*keplerv1.ResolveApprovalResponse, error) {
	if req.GetSessionId() == "" || req.GetToolCallId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id and tool_call_id are required")
	}
	if _, _, err := s.store.ResolveApproval(ctx, req.GetSessionId(), req.GetToolCallId(), req.GetApproved(), req.GetReason()); err != nil {
		return nil, mapStoreError(err)
	}
	s.broadcaster.Notify(req.GetSessionId())
	s.notifyTask()
	return &keplerv1.ResolveApprovalResponse{}, nil
}

func (s *SessionService) ResumeSession(ctx context.Context, req *keplerv1.ResumeSessionRequest) (*keplerv1.ResumeSessionResponse, error) {
	if req.GetSessionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if _, err := s.store.ResumeSession(ctx, req.GetSessionId()); err != nil {
		return nil, mapStoreError(err)
	}
	s.broadcaster.Notify(req.GetSessionId())
	s.notifyTask()
	return &keplerv1.ResumeSessionResponse{}, nil
}

func (s *SessionService) CancelSession(ctx context.Context, req *keplerv1.CancelSessionRequest) (*keplerv1.CancelSessionResponse, error) {
	if req.GetSessionId() == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if err := s.store.CancelSession(ctx, req.GetSessionId()); err != nil {
		return nil, mapStoreError(err)
	}
	s.broadcaster.Notify(req.GetSessionId())
	return &keplerv1.CancelSessionResponse{}, nil
}

func (s *SessionService) StreamEvents(ctx context.Context, req *keplerv1.StreamEventsRequest, stream keplerv1connect.SessionServiceStreamEventsServerStream) error {
	sessionID := req.GetSessionId()
	if sessionID == "" {
		return connect.NewError(connect.CodeInvalidArgument, "session_id is required")
	}
	if req.GetSinceSeq() < 0 {
		return connect.NewError(connect.CodeInvalidArgument, "since_seq must be non-negative")
	}

	// Subscribe BEFORE initial query so no event committed in between is missed.
	notifyCh, unsub := s.broadcaster.Subscribe(sessionID)
	defer unsub()

	lastSeq := req.GetSinceSeq()
	lastStatus := ""

	flush := func() error {
		sess, _, err := s.store.GetSession(ctx, sessionID)
		if err != nil {
			return mapStoreError(err)
		}
		evs, err := s.store.ListEvents(ctx, sessionID, lastSeq)
		if err != nil {
			return mapStoreError(err)
		}
		for _, ev := range evs {
			pbEv, err := EventToProto(ev)
			if err != nil {
				return connect.NewError(connect.CodeInternal, err.Error())
			}
			if err := stream.Send(&keplerv1.StreamEventsResponse{
				Frame: &keplerv1.StreamEventsResponse_Event{Event: pbEv},
			}); err != nil {
				return err
			}
			lastSeq = ev.Seq
		}
		if sess.Status != lastStatus {
			lastStatus = sess.Status
			if err := stream.Send(&keplerv1.StreamEventsResponse{
				Frame: &keplerv1.StreamEventsResponse_Status{
					Status: &keplerv1.StatusFrame{Status: sessionStatusToProto(sess.Status)},
				},
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err := flush(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-notifyCh:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
