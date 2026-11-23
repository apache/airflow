// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package execution

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/contexttest"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/sdk"
)

// TestServeSendsExecutionTimeoutEndToEnd pins that the runtime sends the supervisor the
// execution_timeout in ti_context before the task runs, and sends nothing when there is none or it is
// zero, which means no timeout.
func TestServeSendsExecutionTimeoutEndToEnd(t *testing.T) {
	setTimeout := func(seconds float64) []map[string]any {
		return []map[string]any{{"type": "SetExecutionTimeout", "timeout_seconds": seconds}}
	}
	tests := []struct {
		name         string
		fromContext  any
		wantRequests []map[string]any
	}{
		{name: "whole seconds", fromContext: 30, wantRequests: setTimeout(30)},
		{name: "fractional seconds", fromContext: 1.5, wantRequests: setTimeout(1.5)},
		{name: "zero", fromContext: 0},
		{name: "unset"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ran := false
			task, err := bundle.NewTaskFunction(func(contexttest.Context) error {
				ran = true
				return nil
			})
			require.NoError(t, err)
			tiContext := map[string]any{}
			if tt.fromContext != nil {
				tiContext["execution_timeout"] = tt.fromContext
			}

			requests, terminal := serveTask(t, "task", task, nil, tiContext, answerAll)

			assert.True(t, ran)
			assert.Equal(t, tt.wantRequests, requests)
			assert.Equal(t, "SucceedTask", terminal["type"])
		})
	}
}

// TestServeFailsWithoutRunningOnExecutionTimeoutEndToEnd pins that the task does not run, and
// nothing is sent to the supervisor, when its execution_timeout has already elapsed, as for a Python
// task, or is not a number.
func TestServeFailsWithoutRunningOnExecutionTimeoutEndToEnd(t *testing.T) {
	tests := []struct {
		name        string
		fromContext any
		wantReason  string
	}{
		{
			name:        "negative",
			fromContext: -5,
			wantReason:  "execution_timeout of -5 seconds has already elapsed",
		},
		{
			name:        "not a number",
			fromContext: "30",
			wantReason:  "ti_context.execution_timeout has unexpected wire shape string",
		},
	}
	for _, tt := range tests {
		for _, shouldRetry := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/should_retry=%t", tt.name, shouldRetry), func(t *testing.T) {
				ran := false
				task, err := bundle.NewTaskFunction(func(contexttest.Context) error {
					ran = true
					return nil
				})
				require.NoError(t, err)
				tiContext := map[string]any{
					"should_retry":      shouldRetry,
					"execution_timeout": tt.fromContext,
				}

				requests, terminal := serveTask(t, "task", task, nil, tiContext, answerAll)

				assert.False(t, ran, "the task must not run")
				assert.Empty(t, requests)
				if shouldRetry {
					assert.Equal(t, "RetryTask", terminal["type"])
					assert.Contains(t, terminal["retry_reason"], tt.wantReason)
				} else {
					assert.Equal(t, "TaskState", terminal["type"])
					assert.Equal(t, "failed", terminal["state"])
				}
			})
		}
	}
}

// TestServeFailsWhenTheSupervisorRejectsExecutionTimeoutEndToEnd pins that the task does not run
// when the supervisor rejects SetExecutionTimeout.
func TestServeFailsWhenTheSupervisorRejectsExecutionTimeoutEndToEnd(t *testing.T) {
	for _, shouldRetry := range []bool{true, false} {
		t.Run(fmt.Sprintf("should_retry=%t", shouldRetry), func(t *testing.T) {
			ran := false
			task, err := bundle.NewTaskFunction(func(contexttest.Context) error {
				ran = true
				return nil
			})
			require.NoError(t, err)

			requests, terminal := serveTask(t, "task", task, nil, map[string]any{
				"execution_timeout": 30,
				"should_retry":      shouldRetry,
			}, func(map[string]any) map[string]any {
				return map[string]any{
					"type":   "ErrorResponse",
					"error":  "GENERIC_ERROR",
					"detail": map[string]any{"message": "boom"},
				}
			})

			assert.False(t, ran, "the task must not run")
			assert.Equal(t, []map[string]any{
				{"type": "SetExecutionTimeout", "timeout_seconds": float64(30)},
			}, requests)
			if shouldRetry {
				assert.Equal(t, "RetryTask", terminal["type"])
				assert.Contains(t, terminal["retry_reason"], "GENERIC_ERROR")
			} else {
				assert.Equal(t, "TaskState", terminal["type"])
				assert.Equal(t, "failed", terminal["state"])
			}
		})
	}
}

// TestServeFailsATaskAtItsExecutionTimeoutEndToEnd pins that the runtime cancels the task's ctx at
// its execution_timeout with sdk.ErrTaskTimeout as the cause, and fails the task with the timeout
// whatever the task returns after it. A value returned after the timeout is not pushed as an XCom.
func TestServeFailsATaskAtItsExecutionTimeoutEndToEnd(t *testing.T) {
	tests := []struct {
		name  string
		value string
		err   func(ctx contexttest.Context) error
	}{
		{
			name: "returns the ctx error",
			err:  func(ctx contexttest.Context) error { return ctx.Err() },
		},
		{
			name:  "returns a value",
			value: "too late",
			err:   func(contexttest.Context) error { return nil },
		},
	}
	for _, tt := range tests {
		for _, shouldRetry := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/should_retry=%t", tt.name, shouldRetry), func(t *testing.T) {
				causes := make(chan error, 1)
				task, err := bundle.NewTaskFunction(func(ctx contexttest.Context) (string, error) {
					<-ctx.Done()
					causes <- context.Cause(ctx)
					return tt.value, tt.err(ctx)
				})
				require.NoError(t, err)

				requests, terminal := serveTask(t, "task", task, nil, map[string]any{
					"execution_timeout": 0.05,
					"should_retry":      shouldRetry,
				}, answerAll)

				assert.ErrorIs(t, <-causes, sdk.ErrTaskTimeout)
				assert.Equal(t, []map[string]any{
					{"type": "SetExecutionTimeout", "timeout_seconds": 0.05},
				}, requests)
				if shouldRetry {
					assert.Equal(t, "RetryTask", terminal["type"])
					assert.Equal(t, "task timed out after 0.05 seconds", terminal["retry_reason"])
				} else {
					assert.Equal(t, "TaskState", terminal["type"])
					assert.Equal(t, "failed", terminal["state"])
				}
			})
		}
	}
}

// TestRunWithExecutionTimeoutReportsAtTheSupervisorStop pins that the runtime waits for a task that
// ignores its ctx after the timeout until the supervisor's SIGTERM, and then reports the timeout
// without waiting for the task any longer.
func TestRunWithExecutionTimeoutReportsAtTheSupervisorStop(t *testing.T) {
	stop, sigterm := context.WithCancel(context.Background())
	defer sigterm()
	release := make(chan struct{})
	defer close(release)
	results := make(chan any, 1)
	go func() {
		results <- runWithExecutionTimeout(stop, 0.01, true, discardLogger(), func(context.Context) any {
			<-release
			return succeedTask()
		})
	}()

	select {
	case <-results:
		t.Fatal("the timeout was reported before the supervisor's SIGTERM")
	case <-time.After(100 * time.Millisecond):
	}
	sigterm()

	select {
	case result := <-results:
		assertRetryTask(t, result, "task timed out after 0.01 seconds")
	case <-time.After(5 * time.Second):
		t.Fatal("the runtime kept waiting for the task after the supervisor's SIGTERM")
	}
}

// TestRunWithExecutionTimeoutKeepsTheResultOnAStopBeforeTheTimeout pins that a SIGTERM before the
// timeout is not reported as a timeout: the runtime waits for the task and returns its result.
func TestRunWithExecutionTimeoutKeepsTheResultOnAStopBeforeTheTimeout(t *testing.T) {
	stop, sigterm := context.WithCancel(context.Background())
	causes := make(chan error, 1)
	// The task cannot return before the test closes cleanedUp, so the runtime sees the cancelled
	// ctx first and takes the branch that waits for the task.
	cleanedUp := make(chan struct{})
	results := make(chan any, 1)
	go func() {
		results <- runWithExecutionTimeout(
			stop,
			3600,
			true,
			discardLogger(),
			func(ctx context.Context) any {
				<-ctx.Done()
				causes <- context.Cause(ctx)
				<-cleanedUp
				return succeedTask()
			},
		)
	}()

	sigterm()
	assert.ErrorIs(t, <-causes, context.Canceled)
	close(cleanedUp)

	select {
	case result := <-results:
		assert.IsType(t, genmodels.SucceedTask{}, result)
	case <-time.After(5 * time.Second):
		t.Fatal("the runtime did not return the task's result")
	}
}

func TestSecondsToDuration(t *testing.T) {
	tests := []struct {
		name    string
		seconds float64
		want    time.Duration
	}{
		{name: "fractional seconds", seconds: 1.5, want: 1500 * time.Millisecond},
		{name: "past the longest duration", seconds: 1e300, want: time.Duration(math.MaxInt64)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, secondsToDuration(tt.seconds))
		})
	}
}
