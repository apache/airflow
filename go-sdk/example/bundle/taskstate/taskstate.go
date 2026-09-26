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

// Package taskstate holds the roundtrip_task_state task used by the Go SDK
// task state e2e test.
package taskstate

import (
	"errors"
	"fmt"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/sdk"
)

const (
	// RunIDKey holds the run id; the e2e test asserts it has no expiry.
	RunIDKey = "go_e2e_run_id"
	// CounterKey holds a map value the e2e test reads back as an object.
	CounterKey = "go_e2e_counter"
	// RetainedKey uses the default retention; the e2e test asserts it expires.
	RetainedKey = "go_e2e_retained"
	// ScratchKey is deleted by the task; the e2e test asserts it is gone.
	ScratchKey = "go_e2e_scratch"
)

type counter struct {
	Processed int    `json:"processed"`
	Cursor    string `json:"cursor"`
}

// RoundtripTaskState exercises every task state store operation except
// ClearTaskState.
func RoundtripTaskState(actx airflow.Context) (any, error) {
	client := actx.Client()
	runID := actx.DagRun().RunID

	if err := client.SetTaskStateWithRetention(actx, RunIDKey, runID, sdk.NeverExpire); err != nil {
		return nil, fmt.Errorf("setting %s: %w", RunIDKey, err)
	}
	want := counter{Processed: 3, Cursor: "abc-123"}
	stored := map[string]any{"processed": want.Processed, "cursor": want.Cursor}
	if err := client.SetTaskState(actx, CounterKey, stored); err != nil {
		return nil, fmt.Errorf("setting %s: %w", CounterKey, err)
	}
	if err := client.SetTaskState(actx, RetainedKey, "retained"); err != nil {
		return nil, fmt.Errorf("setting %s: %w", RetainedKey, err)
	}
	if err := client.SetTaskState(actx, ScratchKey, "scratch"); err != nil {
		return nil, fmt.Errorf("setting %s: %w", ScratchKey, err)
	}
	if err := client.DeleteTaskState(actx, ScratchKey); err != nil {
		return nil, fmt.Errorf("deleting %s: %w", ScratchKey, err)
	}

	readBack, err := client.GetTaskState(actx, RunIDKey)
	if err != nil {
		return nil, fmt.Errorf("getting %s: %w", RunIDKey, err)
	}
	if readBack != runID {
		return nil, fmt.Errorf("getting %s: got %v, want run id %q", RunIDKey, readBack, runID)
	}

	var got counter
	if err := client.UnmarshalJSONTaskState(actx, CounterKey, &got); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", CounterKey, err)
	}
	if got != want {
		return nil, fmt.Errorf("decoding %s: got %+v, want %+v", CounterKey, got, want)
	}

	if _, err := client.GetTaskState(actx, ScratchKey); !errors.Is(err, sdk.TaskStateNotFound) {
		if err == nil {
			return nil, fmt.Errorf("getting %s: key survived its delete", ScratchKey)
		}
		return nil, fmt.Errorf("getting %s: want TaskStateNotFound, got: %w", ScratchKey, err)
	}

	// No ClearTaskState: the e2e test reads these keys after the task finishes.
	return map[string]any{
		"run_id":          runID,
		"processed":       got.Processed,
		"cursor":          got.Cursor,
		"scratch_deleted": true,
	}, nil
}
