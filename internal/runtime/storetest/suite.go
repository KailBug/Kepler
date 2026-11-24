package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/KailBug/Kepler/internal/runtime"
)

// Factory creates a clean Store and a controllable clock for each test case.
type Factory func(t *testing.T) (store runtime.Store, advance func(time.Duration))

// Run exercises the full runtime.Store contract against any implementation (memory / postgres).
func Run(t *testing.T, newStore Factory) {
	t.Run("SessionAndUserMessageFlow", func(t *testing.T) {
		ctx := context.Background()
		store, _ := newStore(t)

		sess, err := store.CreateSession(ctx, "default-agent")
		if err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		if sess.Status != runtime.SessionStatusIdle || sess.LastSeq != 0 {
			t.Fatalf("unexpected initial session: %+v", sess)
		}

		turnID, ev, task, err := store.SubmitUserMessage(ctx, sess.ID, "你好，Kepler")
		if err != nil {
			t.Fatalf("SubmitUserMessage: %v", err)
		}
		if turnID == "" || ev.Seq != 1 || ev.Type != "user" || task.Status != runtime.TaskStatusPending {
			t.Fatalf("unexpected submit result: turn=%s ev=%+v task=%+v", turnID, ev, task)
		}

		// Submitting while running must be rejected.
		if _, _, _, err := store.SubmitUserMessage(ctx, sess.ID, "并发输入"); !errors.Is(err, runtime.ErrSessionBusy) {
			t.Fatalf("expected ErrSessionBusy, got %v", err)
		}

		gotSess, gotEvs, err := store.GetSession(ctx, sess.ID)
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if gotSess.Title != "你好，Kepler" || gotSess.LastSeq != 1 || len(gotEvs) != 1 {
			t.Fatalf("unexpected GetSession: sess=%+v evs=%d", gotSess, len(gotEvs))
		}
	})

	t.Run("AcquireLeaseFencingAndZombieRejection", func(t *testing.T) {
		ctx := context.Background()
		store, advance := newStore(t)

		if err := store.RegisterWorker(ctx, runtime.WorkerRegistration{
			WorkerID:     "worker-a",
			Runtime:      "kepler-harness-ts",
			Version:      "0.1.0",
			Capabilities: []string{"default"},
		}); err != nil {
			t.Fatalf("RegisterWorker: %v", err)
		}
		if err := store.HeartbeatWorker(ctx, "worker-a", 1); err != nil {
			t.Fatalf("HeartbeatWorker: %v", err)
		}

		sess, err := store.CreateSession(ctx, "agent-1")
		if err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		turnID, _, _, err := store.SubmitUserMessage(ctx, sess.ID, "计算 2+3")
		if err != nil {
			t.Fatalf("SubmitUserMessage: %v", err)
		}

		// Worker A claims the task.
		acqA, err := store.AcquireTask(ctx, "worker-a", 10*time.Second)
		if err != nil || acqA == nil {
			t.Fatalf("AcquireTask(A): acq=%v err=%v", acqA, err)
		}
		if acqA.Task.Lease.Epoch != 1 || len(acqA.Events) != 1 {
			t.Fatalf("unexpected acqA: %+v", acqA)
		}

		// Worker B gets nothing while lease is valid.
		if acqNone, err := store.AcquireTask(ctx, "worker-b", 10*time.Second); err != nil || acqNone != nil {
			t.Fatalf("expected no claimable task, got acq=%v err=%v", acqNone, err)
		}

		// Worker A renews lease once.
		if _, err := store.RenewLease(ctx, acqA.Task.ID, acqA.Task.Lease.LeaseID, 1, 10*time.Second); err != nil {
			t.Fatalf("RenewLease(A): %v", err)
		}

		// Worker A appends an assistant event (seq=2).
		lastSeq, err := store.AppendEvents(ctx, sess.ID, acqA.Task.Lease.LeaseID, 1, []runtime.Event{{
			TurnID:  turnID,
			Type:    "assistant",
			Payload: json.RawMessage(`{"text":"思考中"}`),
		}})
		if err != nil || lastSeq != 2 {
			t.Fatalf("AppendEvents(A): lastSeq=%d err=%v", lastSeq, err)
		}

		// Bad seq must be rejected with ErrSeqConflict.
		if _, err := store.AppendEvents(ctx, sess.ID, acqA.Task.Lease.LeaseID, 1, []runtime.Event{{
			Seq:     99,
			TurnID:  turnID,
			Type:    "turn-ended",
			Payload: json.RawMessage(`{"status":"completed"}`),
		}}); !errors.Is(err, runtime.ErrSeqConflict) {
			t.Fatalf("expected ErrSeqConflict, got %v", err)
		}

		// Simulate Worker A freeze/crash: advance clock past lease TTL.
		advance(15 * time.Second)

		// Worker B reclaims the task and gets epoch=2 + both events.
		acqB, err := store.AcquireTask(ctx, "worker-b", 10*time.Second)
		if err != nil || acqB == nil {
			t.Fatalf("AcquireTask(B) after expiry: acq=%v err=%v", acqB, err)
		}
		if acqB.Task.Lease.Epoch != 2 || len(acqB.Events) != 2 {
			t.Fatalf("unexpected acqB: epoch=%d events=%d", acqB.Task.Lease.Epoch, len(acqB.Events))
		}

		// Zombie Worker A tries AppendEvents / RenewLease / CompleteTask with stale epoch=1 -> all MUST fail.
		if _, err := store.AppendEvents(ctx, sess.ID, acqA.Task.Lease.LeaseID, 1, []runtime.Event{{
			TurnID:  turnID,
			Type:    "turn-ended",
			Payload: json.RawMessage(`{"status":"completed"}`),
		}}); !errors.Is(err, runtime.ErrStaleEpoch) {
			t.Fatalf("zombie AppendEvents expected ErrStaleEpoch, got %v", err)
		}
		if _, err := store.RenewLease(ctx, acqA.Task.ID, acqA.Task.Lease.LeaseID, 1, 10*time.Second); !errors.Is(err, runtime.ErrStaleEpoch) {
			t.Fatalf("zombie RenewLease expected ErrStaleEpoch, got %v", err)
		}
		if err := store.CompleteTask(ctx, acqA.Task.ID, acqA.Task.Lease.LeaseID, 1); !errors.Is(err, runtime.ErrStaleEpoch) {
			t.Fatalf("zombie CompleteTask expected ErrStaleEpoch, got %v", err)
		}

		// Worker B finishes cleanly with epoch=2.
		lastSeq, err = store.AppendEvents(ctx, sess.ID, acqB.Task.Lease.LeaseID, 2, []runtime.Event{{
			TurnID:  turnID,
			Type:    "turn-ended",
			Payload: json.RawMessage(`{"status":"completed"}`),
		}})
		if err != nil || lastSeq != 3 {
			t.Fatalf("AppendEvents(B): lastSeq=%d err=%v", lastSeq, err)
		}
		if err := store.CompleteTask(ctx, acqB.Task.ID, acqB.Task.Lease.LeaseID, 2); err != nil {
			t.Fatalf("CompleteTask(B): %v", err)
		}

		gotSess, evs, err := store.GetSession(ctx, sess.ID)
		if err != nil {
			t.Fatalf("GetSession: %v", err)
		}
		if gotSess.Status != runtime.SessionStatusIdle || gotSess.LastSeq != 3 || len(evs) != 3 {
			t.Fatalf("unexpected final session: %+v evs=%d", gotSess, len(evs))
		}
	})

	t.Run("SuspendAndResolveApproval", func(t *testing.T) {
		ctx := context.Background()
		store, _ := newStore(t)

		sess, err := store.CreateSession(ctx, "agent-hitl")
		if err != nil {
			t.Fatalf("CreateSession: %v", err)
		}
		turnID, _, _, err := store.SubmitUserMessage(ctx, sess.ID, "写入文件")
		if err != nil {
			t.Fatalf("SubmitUserMessage: %v", err)
		}

		acq, err := store.AcquireTask(ctx, "worker-1", 10*time.Second)
		if err != nil || acq == nil {
			t.Fatalf("AcquireTask: %v", err)
		}
		if _, err := store.AppendEvents(ctx, sess.ID, acq.Task.Lease.LeaseID, acq.Task.Lease.Epoch, []runtime.Event{
			{TurnID: turnID, Type: "approval-requested", Payload: json.RawMessage(`{"toolCallId":"call-1","name":"write_file"}`)},
			{TurnID: turnID, Type: "turn-ended", Payload: json.RawMessage(`{"status":"suspended"}`)},
		}); err != nil {
			t.Fatalf("AppendEvents: %v", err)
		}
		if err := store.SuspendTask(ctx, acq.Task.ID, acq.Task.Lease.LeaseID, acq.Task.Lease.Epoch, "needs approval"); err != nil {
			t.Fatalf("SuspendTask: %v", err)
		}

		// User approves -> new pending task is queued with approval-resolved event.
		resEv, _, err := store.ResolveApproval(ctx, sess.ID, "call-1", true, "approved by admin")
		if err != nil {
			t.Fatalf("ResolveApproval: %v", err)
		}
		if resEv.Seq != 4 || resEv.Type != "approval-resolved" {
			t.Fatalf("unexpected approval-resolved event: %+v", resEv)
		}

		resumed, err := store.AcquireTask(ctx, "worker-2", 10*time.Second)
		if err != nil || resumed == nil {
			t.Fatalf("AcquireTask(resumed): acq=%v err=%v", resumed, err)
		}
		if len(resumed.Events) != 4 {
			t.Fatalf("expected 4 events on resume, got %d", len(resumed.Events))
		}
	})
}
