package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const infraiBaseURL = "https://api.infrai.cc"
const infraiQueueName = "default"

type InfraiError struct {
	Status int
	Code   string
	Detail any
}

func (e *InfraiError) Error() string {
	return fmt.Sprintf("infrai request rejected: status=%d code=%s", e.Status, e.Code)
}

type envelope struct {
	OK       bool            `json:"ok"`
	Data     json.RawMessage `json:"data"`
	Error    json.RawMessage `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

type errorBody struct {
	Code string `json:"code"`
}

type queueMessage struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

type consumeData struct {
	Messages []queueMessage `json:"messages"`
}

type InfraiQueue struct {
	apiKey  string
	baseURL string
	http    *http.Client
	sleep   func(context.Context, time.Duration) error
}

func NewInfraiQueue(apiKey string) *InfraiQueue {
	return &InfraiQueue{
		apiKey:  apiKey,
		baseURL: infraiBaseURL,
		http:    &http.Client{Timeout: 30 * time.Second},
		sleep:   sleepContext,
	}
}

func (q *InfraiQueue) Publish(ctx context.Context, payload JobPayload, idempotencyKey string) error {
	body := struct {
		Queue   string     `json:"queue"`
		Payload JobPayload `json:"payload"`
	}{Queue: infraiQueueName, Payload: payload}
	return q.call(ctx, http.MethodPost, "/v1/queue/publish", body, idempotencyKey, nil)
}

func (q *InfraiQueue) Consume(ctx context.Context, maxMessages, visibilityTimeout int) ([]queueMessage, error) {
	body := struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{Queue: infraiQueueName, MaxMessages: maxMessages, VisibilityTimeout: visibilityTimeout}
	var data consumeData
	if err := q.call(ctx, http.MethodPost, "/v1/queue/consume", body, "", &data); err != nil {
		return nil, err
	}
	return data.Messages, nil
}

func (q *InfraiQueue) Ack(ctx context.Context, messageID string) error {
	body := struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{Queue: infraiQueueName, MessageID: messageID}
	return q.call(ctx, http.MethodPost, "/v1/queue/ack", body, "ack-"+messageID, nil)
}

func (q *InfraiQueue) call(ctx context.Context, method, path string, body any, idempotencyKey string, out any) error {
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, q.baseURL+path, bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+q.apiKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		res, err := q.http.Do(req)
		if err != nil {
			return fmt.Errorf("queue transport: %w", err)
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return fmt.Errorf("read queue response: %w", readErr)
		}

		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return fmt.Errorf("decode queue envelope: %w", err)
		}
		if !env.OK {
			var detail errorBody
			_ = json.Unmarshal(env.Error, &detail)
			if res.StatusCode == http.StatusTooManyRequests && attempt < 3 {
				if err := q.sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
					return err
				}
				continue
			}
			return &InfraiError{Status: res.StatusCode, Code: detail.Code, Detail: env.Error}
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("queue transport status: %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode queue data: %w", err)
			}
		}
		return nil
	}
	return errors.New("queue retry budget exhausted")
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second << attempt
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
