package runtime

import (
	"encoding/json"
	"fmt"

	keplerv1 "github.com/KailBug/Kepler/gen/go/kepler/v1"
)

type userPayload struct {
	Text string `json:"text"`
}

type toolCallPayload struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type assistantPayload struct {
	Text      string            `json:"text"`
	ToolCalls []toolCallPayload `json:"toolCalls,omitempty"`
}

type toolResultPayload struct {
	ToolCallID string `json:"toolCallId"`
	Name       string `json:"name"`
	Content    string `json:"content"`
	IsError    bool   `json:"isError,omitempty"`
}

type approvalRequestedPayload struct {
	ToolCallID string `json:"toolCallId"`
	Name       string `json:"name"`
	Arguments  string `json:"arguments"`
	Reason     string `json:"reason,omitempty"`
}

type approvalResolvedPayload struct {
	ToolCallID string `json:"toolCallId"`
	Approved   bool   `json:"approved"`
	Reason     string `json:"reason,omitempty"`
}

type turnEndedPayload struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// SessionToProto converts a storage Session to kepler.v1.SessionInfo.
func SessionToProto(s Session) *keplerv1.SessionInfo {
	return &keplerv1.SessionInfo{
		Id:             s.ID,
		AgentId:        s.AgentID,
		Title:          s.Title,
		Status:         sessionStatusToProto(s.Status),
		CreatedAtMs:    s.CreatedAtMs,
		UpdatedAtMs:    s.UpdatedAtMs,
		LastSeq:        s.LastSeq,
		ActiveWorkerId: s.ActiveWorkerID,
		LeaseEpoch:     s.LeaseEpoch,
	}
}

// LeaseToProto converts a storage Lease to kepler.v1.Lease.
func LeaseToProto(l Lease) *keplerv1.Lease {
	return &keplerv1.Lease{
		LeaseId:     l.LeaseID,
		Epoch:       l.Epoch,
		ExpiresAtMs: l.ExpiresAtMs,
	}
}

// EventToProto converts a storage Event to kepler.v1.SessionEvent.
func EventToProto(ev Event) (*keplerv1.SessionEvent, error) {
	out := &keplerv1.SessionEvent{
		Id:        ev.ID,
		Seq:       ev.Seq,
		SessionId: ev.SessionID,
		TurnId:    ev.TurnID,
		TsMs:      ev.TsMs,
	}
	switch ev.Type {
	case "user":
		var p userPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return nil, fmt.Errorf("decode user payload: %w", err)
		}
		out.Payload = &keplerv1.SessionEvent_User{User: &keplerv1.UserEvent{Text: p.Text}}
	case "assistant":
		var p assistantPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return nil, fmt.Errorf("decode assistant payload: %w", err)
		}
		calls := make([]*keplerv1.ToolCall, len(p.ToolCalls))
		for i, c := range p.ToolCalls {
			calls[i] = &keplerv1.ToolCall{Id: c.ID, Name: c.Name, Arguments: c.Arguments}
		}
		out.Payload = &keplerv1.SessionEvent_Assistant{
			Assistant: &keplerv1.AssistantEvent{Text: p.Text, ToolCalls: calls},
		}
	case "tool-result":
		var p toolResultPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return nil, fmt.Errorf("decode tool-result payload: %w", err)
		}
		out.Payload = &keplerv1.SessionEvent_ToolResult{
			ToolResult: &keplerv1.ToolResultEvent{
				ToolCallId: p.ToolCallID,
				Name:       p.Name,
				Content:    p.Content,
				IsError:    p.IsError,
			},
		}
	case "approval-requested":
		var p approvalRequestedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return nil, fmt.Errorf("decode approval-requested payload: %w", err)
		}
		out.Payload = &keplerv1.SessionEvent_ApprovalRequested{
			ApprovalRequested: &keplerv1.ApprovalRequestedEvent{
				ToolCallId: p.ToolCallID,
				Name:       p.Name,
				Arguments:  p.Arguments,
				Reason:     p.Reason,
			},
		}
	case "approval-resolved":
		var p approvalResolvedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return nil, fmt.Errorf("decode approval-resolved payload: %w", err)
		}
		out.Payload = &keplerv1.SessionEvent_ApprovalResolved{
			ApprovalResolved: &keplerv1.ApprovalResolvedEvent{
				ToolCallId: p.ToolCallID,
				Approved:   p.Approved,
				Reason:     p.Reason,
			},
		}
	case "turn-ended":
		var p turnEndedPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return nil, fmt.Errorf("decode turn-ended payload: %w", err)
		}
		out.Payload = &keplerv1.SessionEvent_TurnEnded{
			TurnEnded: &keplerv1.TurnEndedEvent{
				Status: turnStatusToProto(p.Status),
				Error:  p.Error,
			},
		}
	default:
		return nil, fmt.Errorf("unknown event type %q", ev.Type)
	}
	return out, nil
}

// EventFromProto converts kepler.v1.SessionEvent to a storage Event.
func EventFromProto(pb *keplerv1.SessionEvent) (Event, error) {
	if pb == nil {
		return Event{}, fmt.Errorf("nil session event")
	}
	ev := Event{
		ID:        pb.GetId(),
		Seq:       pb.GetSeq(),
		SessionID: pb.GetSessionId(),
		TurnID:    pb.GetTurnId(),
		TsMs:      pb.GetTsMs(),
	}
	var (
		raw []byte
		err error
	)
	switch p := pb.GetPayload().(type) {
	case *keplerv1.SessionEvent_User:
		ev.Type = "user"
		raw, err = json.Marshal(userPayload{Text: p.User.GetText()})
	case *keplerv1.SessionEvent_Assistant:
		ev.Type = "assistant"
		calls := make([]toolCallPayload, len(p.Assistant.GetToolCalls()))
		for i, c := range p.Assistant.GetToolCalls() {
			calls[i] = toolCallPayload{ID: c.GetId(), Name: c.GetName(), Arguments: c.GetArguments()}
		}
		raw, err = json.Marshal(assistantPayload{Text: p.Assistant.GetText(), ToolCalls: calls})
	case *keplerv1.SessionEvent_ToolResult:
		ev.Type = "tool-result"
		raw, err = json.Marshal(toolResultPayload{
			ToolCallID: p.ToolResult.GetToolCallId(),
			Name:       p.ToolResult.GetName(),
			Content:    p.ToolResult.GetContent(),
			IsError:    p.ToolResult.GetIsError(),
		})
	case *keplerv1.SessionEvent_ApprovalRequested:
		ev.Type = "approval-requested"
		raw, err = json.Marshal(approvalRequestedPayload{
			ToolCallID: p.ApprovalRequested.GetToolCallId(),
			Name:       p.ApprovalRequested.GetName(),
			Arguments:  p.ApprovalRequested.GetArguments(),
			Reason:     p.ApprovalRequested.GetReason(),
		})
	case *keplerv1.SessionEvent_ApprovalResolved:
		ev.Type = "approval-resolved"
		raw, err = json.Marshal(approvalResolvedPayload{
			ToolCallID: p.ApprovalResolved.GetToolCallId(),
			Approved:   p.ApprovalResolved.GetApproved(),
			Reason:     p.ApprovalResolved.GetReason(),
		})
	case *keplerv1.SessionEvent_TurnEnded:
		ev.Type = "turn-ended"
		raw, err = json.Marshal(turnEndedPayload{
			Status: turnStatusFromProto(p.TurnEnded.GetStatus()),
			Error:  p.TurnEnded.GetError(),
		})
	default:
		return Event{}, fmt.Errorf("missing or unsupported event payload")
	}
	if err != nil {
		return Event{}, err
	}
	ev.Payload = raw
	return ev, nil
}

func sessionStatusToProto(s string) keplerv1.SessionStatus {
	switch s {
	case SessionStatusIdle:
		return keplerv1.SessionStatus_SESSION_STATUS_IDLE
	case SessionStatusRunning:
		return keplerv1.SessionStatus_SESSION_STATUS_RUNNING
	case SessionStatusWaitingApproval:
		return keplerv1.SessionStatus_SESSION_STATUS_WAITING_APPROVAL
	default:
		return keplerv1.SessionStatus_SESSION_STATUS_UNSPECIFIED
	}
}

func turnStatusToProto(s string) keplerv1.TurnStatus {
	switch s {
	case "completed":
		return keplerv1.TurnStatus_TURN_STATUS_COMPLETED
	case "max-steps":
		return keplerv1.TurnStatus_TURN_STATUS_MAX_STEPS
	case "suspended":
		return keplerv1.TurnStatus_TURN_STATUS_SUSPENDED
	case "failed":
		return keplerv1.TurnStatus_TURN_STATUS_FAILED
	case "cancelled":
		return keplerv1.TurnStatus_TURN_STATUS_CANCELLED
	default:
		return keplerv1.TurnStatus_TURN_STATUS_UNSPECIFIED
	}
}

func turnStatusFromProto(s keplerv1.TurnStatus) string {
	switch s {
	case keplerv1.TurnStatus_TURN_STATUS_COMPLETED:
		return "completed"
	case keplerv1.TurnStatus_TURN_STATUS_MAX_STEPS:
		return "max-steps"
	case keplerv1.TurnStatus_TURN_STATUS_SUSPENDED:
		return "suspended"
	case keplerv1.TurnStatus_TURN_STATUS_FAILED:
		return "failed"
	case keplerv1.TurnStatus_TURN_STATUS_CANCELLED:
		return "cancelled"
	default:
		return "failed"
	}
}
