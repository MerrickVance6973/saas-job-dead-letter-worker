package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	queue := NewInfraiQueue(apiKey)
	store := &DeadLetterStore{}
	worker := &Worker{queue: queue, store: store, run: applySaaSOperation, now: time.Now}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go consumeLoop(ctx, worker)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", publishJob(queue))
	mux.HandleFunc("GET /admin/dead-letters", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, store.List())
	})
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Println("SaaS job service listening on :8080")
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func publishJob(queue *InfraiQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var job JobPayload
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil || job.JobID == "" || job.TenantID == "" || job.Operation == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "job_id, tenant_id, and operation are required"})
			return
		}
		job.Attempt = 1
		job.State = "queued"
		if err := queue.Publish(r.Context(), job, "job-"+job.JobID); err != nil {
			var rejected *InfraiError
			if errors.As(err, &rejected) && rejected.Status >= 400 && rejected.Status < 500 {
				writeJSON(w, rejected.Status, map[string]string{"error": rejected.Code})
				return
			}
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "queue request failed"})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"job_id": job.JobID, "state": job.State})
	}
}

func consumeLoop(ctx context.Context, worker *Worker) {
	for ctx.Err() == nil {
		messages, err := worker.queue.Consume(ctx, 10, 30)
		if err != nil {
			log.Printf("consume: %v", err)
			wait(ctx, time.Second)
			continue
		}
		if len(messages) == 0 {
			wait(ctx, time.Second)
			continue
		}
		for _, message := range messages {
			if err := worker.Process(ctx, message); err != nil {
				log.Printf("message %s: %v", message.MessageID, err)
			}
		}
	}
}

func applySaaSOperation(_ context.Context, job JobPayload) error {
	var input struct {
		ForceFailure bool `json:"force_failure"`
	}
	if len(job.Input) > 0 {
		if err := json.Unmarshal(job.Input, &input); err != nil {
			return fmt.Errorf("invalid operation input: %w", err)
		}
	}
	if input.ForceFailure {
		return errors.New("operation rejected by tenant policy")
	}
	log.Printf("applied %s for tenant %s", job.Operation, job.TenantID)
	return nil
}

func wait(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
