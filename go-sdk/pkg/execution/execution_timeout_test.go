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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/contexttest"
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
