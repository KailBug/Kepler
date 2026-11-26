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

func TestSessionServiceStreamAndWorkerEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store := memory.New()
	bc := runtime.NewBroadcaster()
	workerSvc := runtime.NewWorkerService(store, runtime.WorkerServiceOptions{
		Broadcaster:  bc,
		LeaseTTL:     10 * time.Second,
		PollInterval: 20 * time.Millisecond,
	})
	sessionSvc := runtime.NewSessionService(store, runtime.SessionServiceOptions{
		Broadcaster:   bc,
		OnTaskCreated: workerSvc.NotifyNewTask,
	})

	cs := connect.NewServer()
	keplerv1connect.RegisterWorkerServiceHandler(cs, workerSvc)
	keplerv1connect.RegisterSessionServiceHandler(cs, sessionSvc)
	mux := http.NewServeMux()
	connecthttp.Mount(mux, cs)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cc := connect.NewClient(connecthttp.NewTransport(ts.Client(), ts.URL))
	sessions := keplerv1connect.NewSessionServiceClient(cc)
	workers := keplerv1connect.NewWorkerServiceClient(cc)

	// 1. CreateSession + SendMessage
	created, err := sessions.CreateSession(ctx, &keplerv1.CreateSessionRequest{AgentId: "kepler-default"})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	sessID := created.GetSession().GetId()

	sent, err := sessions.SendMessage(ctx, &keplerv1.SendMessageRequest{SessionId: sessID, Text: "你好"})
	if err != nil || sent.GetTurnId() == "" {
		t.Fatalf("SendMessage: resp=%v err=%v", sent, err)
	}

	// 2. Subscribe StreamEvents from seq 0
	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	stream, err := sessions.StreamEvents(streamCtx, &keplerv1.StreamEventsRequest{
		SessionId: sessID,
		SinceSeq:  0,
	})
	if err != nil {
		t.Fatalf("StreamEvents: %v", err)
	}
	defer stream.Close()

	// Should replay user event (seq=1) and status=RUNNING immediately.
	f1, err := stream.Receive()
	if err != nil || f1.GetEvent().GetSeq() != 1 || f1.GetEvent().GetUser().GetText() != "你好" {
		t.Fatalf("frame 1 = %v, err = %v", f1, err)
	}
	f2, err := stream.Receive()
	if err != nil || f2.GetStatus().GetStatus() != keplerv1.SessionStatus_SESSION_STATUS_RUNNING {
		t.Fatalf("frame 2 = %v, err = %v", f2, err)
	}

	// 3. Worker acquires task, appends assistant + turn-ended, completes task.
	acq, err := workers.AcquireTask(ctx, &keplerv1.AcquireTaskRequest{WorkerId: "w1", WaitTimeoutMs: 500})
	if err != nil || acq.GetTask() == nil {
		t.Fatalf("AcquireTask: resp=%v err=%v", acq, err)
	}
	task := acq.GetTask()
	if _, err := workers.AppendEvents(ctx, &keplerv1.AppendEventsRequest{
		SessionId: sessID,
		LeaseId:   task.GetLease().GetLeaseId(),
		Epoch:     task.GetLease().GetEpoch(),
		Events: []*keplerv1.SessionEvent{
			{TurnId: task.GetTurnId(), Payload: &keplerv1.SessionEvent_Assistant{Assistant: &keplerv1.AssistantEvent{Text: "你好，我是 Kepler"}}},
			{TurnId: task.GetTurnId(), Payload: &keplerv1.SessionEvent_TurnEnded{TurnEnded: &keplerv1.TurnEndedEvent{Status: keplerv1.TurnStatus_TURN_STATUS_COMPLETED}}},
		},
	}); err != nil {
		t.Fatalf("AppendEvents: %v", err)
	}
	if _, err := workers.CompleteTask(ctx, &keplerv1.CompleteTaskRequest{
		TaskId:  task.GetTaskId(),
		LeaseId: task.GetLease().GetLeaseId(),
		Epoch:   task.GetLease().GetEpoch(),
	}); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	// 4. Stream receives seq=2, seq=3, and status=IDLE in real time.
	var gotEvents []*keplerv1.SessionEvent
	gotIdle := false
	for !gotIdle {
		frame, err := stream.Receive()
		if err != nil {
			t.Fatalf("stream.Receive: %v", err)
		}
		if ev := frame.GetEvent(); ev != nil {
			gotEvents = append(gotEvents, ev)
		}
		if st := frame.GetStatus(); st != nil && st.GetStatus() == keplerv1.SessionStatus_SESSION_STATUS_IDLE {
			gotIdle = true
		}
	}
	if len(gotEvents) != 2 || gotEvents[0].GetSeq() != 2 || gotEvents[0].GetAssistant().GetText() != "你好，我是 Kepler" || gotEvents[1].GetSeq() != 3 {
		t.Fatalf("unexpected live events: %+v", gotEvents)
	}
}
