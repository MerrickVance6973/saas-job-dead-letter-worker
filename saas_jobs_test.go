package main

import "testing"

func TestClassifyFailure(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		attempt   int
		want      FailureAction
	}{
		{"onboarding gets another delivery", "tenant_onboarding", 2, ActionRetry},
		{"onboarding reaches dead letter", "tenant_onboarding", 3, ActionDeadLetter},
		{"account suspension has a shorter budget", "account_suspend", 2, ActionDeadLetter},
		{"admin export retries once", "admin_export", 1, ActionRetry},
		{"unknown operation is isolated", "billing_rebuild", 1, ActionDeadLetter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyFailure(tt.operation, tt.attempt); got != tt.want {
				t.Fatalf("classifyFailure(%q, %d) = %q, want %q", tt.operation, tt.attempt, got, tt.want)
			}
		})
	}
}
