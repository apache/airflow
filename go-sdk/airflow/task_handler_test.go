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

package airflow

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/pkg/execution"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

func literalArg(name, jsonType string, value any) map[string]any {
	return map[string]any{
		"name":         name,
		"kind":         "literal",
		"value_schema": map[string]any{"type": jsonType},
		"value":        value,
	}
}

func startupDetails(
	dagId, taskId string,
	args ...genmodels.TaskArgBinding,
) *genmodels.StartupDetails {
	mapIndex := -1
	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:        "550e8400-e29b-41d4-a716-446655440000",
			DagID:     dagId,
			TaskID:    taskId,
			RunID:     "run1",
			TryNumber: 2,
			MapIndex:  &mapIndex,
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}
	if args != nil {
		bindings := genmodels.ArgBindings(args)
		details.TIContext.ArgBindings = &bindings
	}
	return details
}

// runTask runs one registered task the way the coordinator runtime does after Serve.
func runTask(
	ctx context.Context,
	b *BundleRef,
	logger *slog.Logger,
	details *genmodels.StartupDetails,
) any {
	comm := execution.NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)
	return execution.RunTask(ctx, &b.taskHandlers, details, comm, logger)
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func panicMessage(t *testing.T, f func()) (msg string) {
	t.Helper()
	defer func() { msg = fmt.Sprint(recover()) }()
	f()
	t.Fatal("expected a panic")
	return ""
}

func TestTaskHandlerPanicsOnBadHandler(t *testing.T) {
	var unassigned func(Context) error

	tests := []struct {
		name string
		fn   any
		want string
	}{
		{name: "not a function", fn: "transform", want: "fn is string, not a function"},
		{name: "nil", fn: nil, want: "fn is <nil>, not a function"},
		{
			name: "nil function value",
			fn:   unassigned,
			want: "fn is a nil func(airflow.Context) error",
		},
		{
			name: "no leading Context",
			fn:   func(ctx context.Context, logger *slog.Logger) error { return nil },
			want: "parameter 0 is context.Context, but the first parameter must be airflow.Context",
		},
		{
			name: "Context by pointer",
			fn:   func(*Context) error { return nil },
			want: "parameter 0 is *airflow.Context, but airflow.Context is taken by value",
		},
		{
			name: "no error result",
			fn:   func(Context) int { return 0 },
			want: "last return value to return error but found int",
		},
		{
			name: "parameter that cannot hold an argument",
			fn:   func(Context, chan int) error { return nil },
			want: "type chan int cannot receive a task argument",
		},
		{
			name: "variadic",
			fn:   func(Context, ...string) error { return nil },
			want: "is variadic; a task argument cannot fill a ... parameter",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := panicMessage(t, func() { TaskHandler("py_etl", "transform", tt.fn) })
			assert.Contains(t, msg, `airflow.TaskHandler("py_etl", "transform"): `)
			assert.Contains(t, msg, tt.want)
		})
	}
}

func TestTaskHandlerRunsWithContextAndArguments(t *testing.T) {
	var got Context
	var gotCountry string
	loadRan := false

	b := Bundle()
	b.Register(
		TaskHandler("py_etl", "transform", func(actx Context, country string) error {
			got = actx
			gotCountry = country
			return nil
		}),
		TaskHandler("py_etl", "load", func(Context) error {
			loadRan = true
			return nil
		}),
	)

	logger := discardLogger()
	details := startupDetails("py_etl", "transform", literalArg("country", "string", "uk"))
	result := runTask(context.Background(), b, logger, details)

	require.IsType(t, genmodels.SucceedTask{}, result)
	assert.Equal(t, "uk", gotCountry)
	assert.False(t, loadRan, "only the task the runtime asked for may run")

	assert.Same(t, logger, got.Logger())
	assert.NotNil(t, got.Client())
	assert.Equal(t, "py_etl", got.TaskInstance().DagID)
	assert.Equal(t, "transform", got.TaskInstance().TaskID)
	assert.Equal(t, 2, got.TaskInstance().TryNumber)
	assert.Equal(t, "run1", got.DagRun().RunID)

	// Only NewContext stores what FromContext reads, so FromContext finds nothing if the runtime
	// builds the Context some other way.
	recovered, ok := FromContext(context.Context(got))
	require.True(t, ok)
	assert.Equal(t, got.TaskInstance(), recovered.TaskInstance())
}

func TestTaskHandlerContextFollowsTaskCancellation(t *testing.T) {
	var sawErr error
	b := Bundle()
	b.Register(TaskHandler("py_etl", "transform", func(actx Context) error {
		sawErr = actx.Err()
		return sawErr
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := runTask(ctx, b, discardLogger(), startupDetails("py_etl", "transform"))

	require.ErrorIs(t, sawErr, context.Canceled)
	state, ok := result.(genmodels.TaskState)
	require.True(t, ok, "expected TaskState, got %T", result)
	assert.Equal(t, genmodels.TaskStateStateFailed, state.State)
}

type taggedInput struct {
	Region    string  `arg:"region_code"`
	Threshold float64 `arg:"threshold"`
}

type untaggedInput struct {
	RegionCode string
	Threshold  float64
}

func TestTaskHandlerBindsArguments(t *testing.T) {
	var gotRegion string
	var gotThreshold float64

	tests := []struct {
		name string
		fn   any
	}{
		{
			name: "flat positional",
			fn: func(_ Context, regionCode string, threshold float64) error {
				gotRegion, gotThreshold = regionCode, threshold
				return nil
			},
		},
		{
			name: "struct with arg tags",
			fn: func(_ Context, in taggedInput) error {
				gotRegion, gotThreshold = in.Region, in.Threshold
				return nil
			},
		},
		{
			name: "struct without tags",
			fn: func(_ Context, in untaggedInput) error {
				gotRegion, gotThreshold = in.RegionCode, in.Threshold
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRegion, gotThreshold = "", 0

			b := Bundle()
			b.Register(TaskHandler("py_etl", "via_args", tt.fn))
			details := startupDetails(
				"py_etl", "via_args",
				literalArg("region_code", "string", "eu-west-1"),
				literalArg("threshold", "number", 0.5),
			)
			result := runTask(context.Background(), b, discardLogger(), details)

			require.IsType(t, genmodels.SucceedTask{}, result)
			assert.Equal(t, "eu-west-1", gotRegion)
			assert.InDelta(t, 0.5, gotThreshold, 0)
		})
	}
}
