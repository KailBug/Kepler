package runtime_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	keplerv1 "github.com/KailBug/Kepler/gen/go/kepler/v1"
	"github.com/KailBug/Kepler/gen/go/kepler/v1/keplerv1connect"
	"github.com/KailBug/Kepler/internal/runtime"
	"github.com/KailBug/Kepler/internal/runtime/memory"
)

func TestWorkerServiceConnectRPC(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	svc := runtime.NewWorkerService(store, runtime.WorkerServiceOptions{
		LeaseTTL:     10 * time.Second,
		PollInterval: 20 * time.Millisecond,
	})

	connectServer := connect.NewServer()
	keplerv1connect.RegisterWorkerServiceHandler(connectServer, svc)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := keplerv1connect.NewWorkerServiceClient(
		connect.NewClient(connecthttp.NewTransport(ts.Client(), ts.URL)),
	)

	// 1. RegisterWorker + Heartbeat
	regResp, err := client.RegisterWorker(ctx, &keplerv1.RegisterWorkerRequest{
		WorkerId:     "worker-ts-1",
		Runtime:      "kepler-harness-ts",
		Version:      "0.1.0",
		Capabilities: []string{"default"},
	})
	if err != nil || regResp.GetHeartbeatIntervalMs() <= 0 {
		t.Fatalf("RegisterWorker: resp=%v err=%v", regResp, err)
	}
	if _, err := client.Heartbeat(ctx, &keplerv1.HeartbeatRequest{WorkerId: "worker-ts-1", ActiveTasks: 0}); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// 2. Long-polling AcquireTask wakes up when a user message arrives
	sess, err := store.CreateSession(ctx, "agent-1")
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	type acqRes struct {
		resp *keplerv1.AcquireTaskResponse
		err  error
	}
	ch := make(chan acqRes, 1)
	go func() {
		resp, err := client.AcquireTask(ctx, &keplerv1.AcquireTaskRequest{
			WorkerId:      "worker-ts-1",
			WaitTimeoutMs: 2000,
		})
		ch <- acqRes{resp: resp, err: err}
	}()

	time.Sleep(40 * time.Millisecond)
	turnID, _, _, err := store.SubmitUserMessage(ctx, sess.ID, "你好 Kepler")
	if err != nil {
		t.Fatalf("SubmitUserMessage: %v", err)
	}
	svc.NotifyNewTask()

	got := <-ch
	if got.err != nil || got.resp.GetTask() == nil {
		t.Fatalf("AcquireTask: resp=%v err=%v", got.resp, got.err)
	}
	task := got.resp.GetTask()
	if task.GetSessionId() != sess.ID || task.GetTurnId() != turnID || task.GetLease().GetEpoch() != 1 || len(task.GetEvents()) != 1 {
		t.Fatalf("unexpected task: %+v", task)
	}

	// 3. RenewLease + AppendEvents + CompleteTask
	if _, err := client.RenewLease(ctx, &keplerv1.RenewLeaseRequest{
		TaskId:  task.GetTaskId(),
		LeaseId: task.GetLease().GetLeaseId(),
		Epoch:   task.GetLease().GetEpoch(),
	}); err != nil {
		t.Fatalf("RenewLease: %v", err)
	}

	appendResp, err := client.AppendEvents(ctx, &keplerv1.AppendEventsRequest{
		SessionId: sess.ID,
		LeaseId:   task.GetLease().GetLeaseId(),
		Epoch:     task.GetLease().GetEpoch(),
		Events: []*keplerv1.SessionEvent{
			{
				TurnId: turnID,
				Payload: &keplerv1.SessionEvent_Assistant{
					Assistant: &keplerv1.AssistantEvent{Text: "你好！"},
				},
			},
			{
				TurnId: turnID,
				Payload: &keplerv1.SessionEvent_TurnEnded{
					TurnEnded: &keplerv1.TurnEndedEvent{Status: keplerv1.TurnStatus_TURN_STATUS_COMPLETED},
				},
			},
		},
	})
	if err != nil || appendResp.GetLastSeq() != 3 {
		t.Fatalf("AppendEvents: resp=%v err=%v", appendResp, err)
	}

	// Stale epoch must return CodeFailedPrecondition.
	_, err = client.AppendEvents(ctx, &keplerv1.AppendEventsRequest{
		SessionId: sess.ID,
		LeaseId:   task.GetLease().GetLeaseId(),
		Epoch:     999,
		Events: []*keplerv1.SessionEvent{{
			TurnId:  turnID,
			Payload: &keplerv1.SessionEvent_Assistant{Assistant: &keplerv1.AssistantEvent{Text: "zombie"}},
		}},
	})
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("stale epoch code = %v (err=%v), want FailedPrecondition", connect.CodeOf(err), err)
	}

	if _, err := client.CompleteTask(ctx, &keplerv1.CompleteTaskRequest{
		TaskId:  task.GetTaskId(),
		LeaseId: task.GetLease().GetLeaseId(),
		Epoch:   task.GetLease().GetEpoch(),
	}); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
}
