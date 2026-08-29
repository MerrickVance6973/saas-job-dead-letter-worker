package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

type JobPayload struct {
	JobID      string          `json:"job_id"`
	TenantID   string          `json:"tenant_id"`
	Operation  string          `json:"operation"`
	Attempt    int             `json:"attempt"`
	State      string          `json:"state"`
	Reason     string          `json:"reason,omitempty"`
	OccurredAt string          `json:"occurred_at,omitempty"`
	Input      json.RawMessage `json:"input,omitempty"`
}

type FailureAction string

const (
	ActionRetry      FailureAction = "retry"
	ActionDeadLetter FailureAction = "dead_letter"
)

func classifyFailure(operation string, attempt int) FailureAction {
	limits := map[string]int{
		"tenant_onboarding": 3,
		"account_suspend":   2,
		"admin_export":      2,
	}
	limit, known := limits[operation]
	if !known || attempt >= limit {
		return ActionDeadLetter
	}
	return ActionRetry
}

type DeadLetter struct {
	JobID      string `json:"job_id"`
	TenantID   string `json:"tenant_id"`
	Operation  string `json:"operation"`
	Attempts   int    `json:"attempts"`
	Reason     string `json:"reason"`
	OccurredAt string `json:"occurred_at"`
}

type DeadLetterStore struct {
	mu    sync.RWMutex
	items []DeadLetter
}

func (s *DeadLetterStore) Add(item DeadLetter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, item)
}

func (s *DeadLetterStore) List() []DeadLetter {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]DeadLetter, len(s.items))
	copy(result, s.items)
	return result
}

type Worker struct {
	queue *InfraiQueue
	store *DeadLetterStore
	run   func(context.Context, JobPayload) error
	now   func() time.Time
}

func (w *Worker) Process(ctx context.Context, message queueMessage) error {
	var job JobPayload
	if err := json.Unmarshal(message.Payload, &job); err != nil {
		return fmt.Errorf("decode job payload: %w", err)
	}
	if job.State == string(ActionDeadLetter) {
		w.store.Add(DeadLetter{job.JobID, job.TenantID, job.Operation, job.Attempt, job.Reason, job.OccurredAt})
		return w.queue.Ack(ctx, message.MessageID)
	}

	if err := w.run(ctx, job); err == nil {
		return w.queue.Ack(ctx, message.MessageID)
	} else if classifyFailure(job.Operation, job.Attempt) == ActionRetry {
		job.Attempt++
		job.State = string(ActionRetry)
		job.Reason = err.Error()
		if publishErr := w.queue.Publish(ctx, job, fmt.Sprintf("retry-%s-%d", job.JobID, job.Attempt)); publishErr != nil {
			return publishErr
		}
	} else {
		job.State = string(ActionDeadLetter)
		job.Reason = err.Error()
		job.OccurredAt = w.now().UTC().Format(time.RFC3339)
		if publishErr := w.queue.Publish(ctx, job, "dead-letter-"+job.JobID); publishErr != nil {
			return publishErr
		}
	}
	return w.queue.Ack(ctx, message.MessageID)
}
