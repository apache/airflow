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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/internal/contexttest"
	"github.com/apache/airflow/go-sdk/pkg/binding"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/sdk"
)

// assertSucceedTask asserts RunTask produced a terminal SucceedTask body.
func assertSucceedTask(t *testing.T, result any) {
	t.Helper()
	_, ok := result.(genmodels.SucceedTask)
	assert.True(t, ok, "expected SucceedTask, got %T", result)
}

// assertTaskState asserts RunTask produced a terminal TaskState body in the
// expected state.
func assertTaskState(t *testing.T, result any, want genmodels.TaskStateState) {
	t.Helper()
	ts, ok := result.(genmodels.TaskState)
	require.True(t, ok, "expected TaskState, got %T", result)
	assert.Equal(t, want, ts.State)
}

// assertRetryTask asserts RunTask produced a RetryTask body whose retry_reason
// contains reasonSubstr.
func assertRetryTask(t *testing.T, result any, reasonSubstr string) {
	t.Helper()
	rt, ok := result.(genmodels.RetryTask)
	require.True(t, ok, "expected RetryTask, got %T", result)
	assert.Contains(t, ifaceString(rt.RetryReason), reasonSubstr)
}

// --- Test task functions ---

func failingTask(contexttest.Context) error {
	return errors.New("task failed intentionally")
}

func panicTask(contexttest.Context) error {
	panic("something went wrong")
}

func simpleTask(contexttest.Context) error {
	return nil
}

func init() { binding.RegisterTaskContext(contexttest.New) }

type testBundle map[string]testDag

type testDag map[string]bundle.Task

func (b testBundle) AddDag(dagID string) testDag {
	b[dagID] = testDag{}
	return b[dagID]
}

func (d testDag) AddTaskWithName(taskID string, fn any) {
	task, err := bundle.NewTaskFunction(fn)
	if err != nil {
		panic(err)
	}
	d[taskID] = task
}

func (b testBundle) LookupTask(dagID, taskID string) (bundle.Task, bool) {
	task, ok := b[dagID][taskID]
	return task, ok
}

func buildBundle(t *testing.T, register func(testBundle)) bundle.Bundle {
	t.Helper()
	b := testBundle{}
	register(b)
	return b
}

func newStartupDetails(
	taskID string,
	bindings ...genmodels.TaskArgBinding,
) *genmodels.StartupDetails {
	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   taskID,
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}
	if bindings != nil {
		specs := genmodels.ArgBindings(bindings)
		details.TIContext.ArgBindings = &specs
	}
	return details
}

func TestTaskRunnerSuccess(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("simpleTask", simpleTask)
	})

	details := newStartupDetails("simpleTask")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)
}

func TestTaskRunnerFailure(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("failingTask", failingTask)
	})

	details := newStartupDetails("failingTask")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestTaskRunnerRetry(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("failingTask", failingTask)
	})

	details := newStartupDetails("failingTask")
	details.TIContext.ShouldRetry = true
	details.TIContext.MaxTries = 3

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertRetryTask(t, result, "task failed intentionally")
}

func TestTaskRunnerTaskNotFound(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("simpleTask", simpleTask)
	})

	details := newStartupDetails("nonexistent")
	details.TIContext.XcomKeysToClear = []string{"return_value"}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var sent bytes.Buffer
	comm := NewCoordinatorComm(bytes.NewReader(nil), &sent, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateRemoved)
	assert.Zero(t, sent.Len(), "a task that is not in the bundle must not delete any XCom")
}

func TestTaskRunnerPanic(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("panicTask", panicTask)
	})

	details := newStartupDetails("panicTask")

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestTaskRunnerPanicRetry(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("panicTask", panicTask)
	})

	details := newStartupDetails("panicTask")
	details.TIContext.ShouldRetry = true
	details.TIContext.MaxTries = 3

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertRetryTask(t, result, "panic: something went wrong")
}

func TestTaskRunnerBindsArgs(t *testing.T) {
	var gotCountry string
	var gotMeta map[string]any
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, country string, meta map[string]any) error {
				gotCountry = country
				gotMeta = meta
				return nil
			})
	})

	details := newStartupDetails(
		"transform",
		map[string]any{
			"name":         "country",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "string"},
			"value":        "uk",
		},
		map[string]any{
			"name":         "meta",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "object"},
			"value":        map[string]any{"k": "v"},
		},
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)
	assert.Equal(t, "uk", gotCountry)
	assert.Equal(t, map[string]any{"k": "v"}, gotMeta)
}

func TestTaskRunnerArgBindingsArityMismatch(t *testing.T) {
	ran := false
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, country string, meta map[string]any) error {
				ran = true
				return nil
			})
	})

	details := newStartupDetails(
		"transform",
		map[string]any{
			"name":         "country",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "string"},
			"value":        "uk",
		},
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
	assert.False(t, ran, "the task body must not run on an arity mismatch")
}

type regionInput struct {
	Region string `arg:"region"`
}

func TestTaskRunnerBindsStructArgs(t *testing.T) {
	var got regionInput
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, input regionInput) error {
				got = input
				return nil
			})
	})

	details := newStartupDetails(
		"transform",
		map[string]any{
			"name":         "region",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "string"},
			"value":        "eu-west-1",
		},
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)
	assert.Equal(t, "eu-west-1", got.Region)
}

func TestTaskRunnerStructIgnoresUnclaimedDefault(t *testing.T) {
	var got regionInput
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, input regionInput) error {
				got = input
				return nil
			})
	})

	details := newStartupDetails(
		"transform",
		map[string]any{
			"name":         "region",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "string"},
			"value":        "eu-west-1",
		},
		map[string]any{
			"name":         "threshold",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "number"},
			"value":        0.75,
			"from_default": true,
		},
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)
	assert.Equal(t, "eu-west-1", got.Region)
}

func TestTaskRunnerArgBindingsTypeMismatch(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, count int) error { return nil })
	})

	details := newStartupDetails(
		"transform",
		map[string]any{
			"name":         "count",
			"kind":         "literal",
			"value_schema": map[string]any{"type": "string"},
			"value":        "uk",
		},
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestTaskRunnerArgBindingsUnknownKind(t *testing.T) {
	ran := false
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, country string) error {
				ran = true
				return nil
			})
	})

	details := newStartupDetails(
		"transform",
		map[string]any{"name": "country", "kind": "template", "value": "x"},
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
	assert.False(t, ran, "the task body must not run on an unknown binding kind")
}

func TestTaskRunnerArgBindingsMalformedElement(t *testing.T) {
	ran := false
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, country string) error {
				ran = true
				return nil
			})
	})

	details := newStartupDetails(
		"transform",
		"bogus",
	)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
	assert.False(t, ran, "the task body must not run on a malformed binding element")
}

func TestTaskRunnerArgBindingsMissingRequiredFields(t *testing.T) {
	cases := []struct {
		name string
		spec map[string]any
	}{
		{name: "missing name", spec: map[string]any{"kind": "literal", "value": "x"}},
		{name: "empty name", spec: map[string]any{"name": "", "kind": "literal", "value": "x"}},
		{name: "xcom missing task_id", spec: map[string]any{"name": "country", "kind": "xcom"}},
		{
			name: "xcom empty task_id",
			spec: map[string]any{"name": "country", "kind": "xcom", "task_id": ""},
		},
		{
			name: "value_schema not a map",
			spec: map[string]any{
				"name": "country", "kind": "literal", "value": "x", "value_schema": "string",
			},
		},
		{
			name: "from_default not a bool",
			spec: map[string]any{
				"name": "country", "kind": "literal", "value": "x", "from_default": "true",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ran := false
			bundle := buildBundle(t, func(r testBundle) {
				r.AddDag("test_dag").AddTaskWithName("transform",
					func(actx contexttest.Context, country string) error {
						ran = true
						return nil
					})
			})

			details := newStartupDetails("transform", tc.spec)

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

			result := RunTask(context.Background(), bundle, details, comm, logger)
			assertTaskState(t, result, genmodels.TaskStateStateFailed)
			assert.False(t, ran, "the task body must not run on an incomplete binding spec")
		})
	}
}

func TestTaskRunnerMalformedSpecHonorsShouldRetry(t *testing.T) {
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(actx contexttest.Context, country string) error { return nil })
	})

	details := newStartupDetails(
		"transform",
		map[string]any{"name": "country", "kind": "template", "value": "x"},
	)
	details.TIContext.ShouldRetry = true

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertRetryTask(t, result, `unknown kind "template"`)
}

// A handler taking an airflow.Context gets on that one value everything
// the runtime used to hand over as separate parameters.
func TestRunTaskInjectsAirflowContext(t *testing.T) {
	logical := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	start := logical.Add(-time.Hour)
	end := logical.Add(time.Hour)

	var got contexttest.Context
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("ctxgrab",
			func(actx contexttest.Context) error {
				got = actx
				return nil
			})
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:        "550e8400-e29b-41d4-a716-446655440000",
			DagID:     "test_dag",
			TaskID:    "ctxgrab",
			RunID:     "run1",
			TryNumber: 2,
			MapIndex:  ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
		// The supervisor nests scheduling timestamps under dag_run; the
		// generated nullable date-time fields hold time.Time values directly.
		TIContext: genmodels.TIRunContext{
			DagRun: genmodels.DagRun{
				LogicalDate:       logical,
				DataIntervalStart: start,
				DataIntervalEnd:   end,
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)

	assert.Same(t, logger, got.Logger(), "the task's logger must arrive on the Context")
	assert.NotNil(t, got.Client(), "the coordinator-backed client must arrive on the Context")

	ti := got.TaskInstance()
	assert.Equal(t, "test_dag", ti.DagID)
	assert.Equal(t, "run1", ti.RunID)
	assert.Equal(t, "ctxgrab", ti.TaskID)
	assert.Equal(t, 2, ti.TryNumber)
	assert.Nil(t, ti.MapIndex, "an unmapped task (map_index -1) must surface as nil")

	dagRun := got.DagRun()
	assert.Equal(t, "test_dag", dagRun.DagID)
	assert.Equal(t, "run1", dagRun.RunID)
	require.NotNil(t, dagRun.LogicalDate)
	assert.Equal(t, logical, *dagRun.LogicalDate)
	require.NotNil(t, dagRun.DataIntervalStart)
	assert.Equal(t, start, *dagRun.DataIntervalStart)
	require.NotNil(t, dagRun.DataIntervalEnd)
	assert.Equal(t, end, *dagRun.DataIntervalEnd)
}

// Guards against the runtime binding the task state store to an empty task instance id.
func TestRunTaskBindsTaskStateStoreClient(t *testing.T) {
	const tiID = "0199e0e5-1b2c-7c3d-8e4f-5a6b7c8d9e0f"

	// A default-retention write needs the setting the supervisor passes.
	t.Setenv(defaultRetentionDaysEnv, "30")

	var got sdk.TaskStateStoreClient
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("statestore",
			func(actx contexttest.Context) error {
				got = actx.Client()
				return actx.Client().TaskStateStore().Set(actx, "job_id", "abc123")
			})
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       tiID,
			DagID:    "test_dag",
			TaskID:   "statestore",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	responsePayload := encodeResponseFrame(t, 0, map[string]any{"type": "OKResponse"}, nil)
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)

	require.NotNil(t, got, "the task must reach a coordinator-backed task state store")

	sent, err := readFrame(&requestBuf)
	require.NoError(t, err)
	sentMap := rawToMap(t, sent.Body)
	assert.Equal(t, "SetTaskStateStore", sentMap["type"])
	assert.Equal(t, tiID, sentMap["ti_id"],
		"the runtime must bind the store to the started task instance")
}

// Serve traps SIGINT/SIGTERM into the context it hands RunTask, so a
// supervisor shutdown reaches the handler on actx.Done().
func TestRunTaskAirflowContextHonorsShutdown(t *testing.T) {
	var sawDone bool
	var sawErr error
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("ctxcheck",
			func(actx contexttest.Context) error {
				select {
				case <-actx.Done():
					sawDone = true
				default:
				}
				sawErr = actx.Err()
				return sawErr
			})
	})

	details := newStartupDetails("ctxcheck")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(ctx, bundle, details, comm, logger)

	assert.True(t, sawDone, "actx.Done() must fire on a cancelled task context")
	assert.ErrorIs(t, sawErr, context.Canceled)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestRunTaskRuntimeContextMappedIndex(t *testing.T) {
	var got contexttest.Context
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("test_dag").AddTaskWithName("ctxgrab",
			func(actx contexttest.Context) error {
				got = actx
				return nil
			})
	})

	details := newStartupDetails("ctxgrab")
	details.TI.MapIndex = ptr(5)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)

	require.NotNil(t, got.TaskInstance().MapIndex, "a mapped task must surface its index")
	assert.Equal(t, 5, *got.TaskInstance().MapIndex)
}

// --- End-to-end Serve test against a fake supervisor ---

func startSupervisor(
	t *testing.T,
) (commAddr, logsAddr string, commCh, logsCh chan net.Conn, cleanup func()) {
	t.Helper()
	commLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	logsLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	commCh = make(chan net.Conn, 1)
	logsCh = make(chan net.Conn, 1)
	go func() {
		c, err := commLn.Accept()
		if err == nil {
			commCh <- c
		}
		close(commCh)
	}()
	go func() {
		c, err := logsLn.Accept()
		if err == nil {
			logsCh <- c
		}
		close(logsCh)
	}()
	cleanup = func() {
		commLn.Close()
		logsLn.Close()
	}
	return commLn.Addr().String(), logsLn.Addr().String(), commCh, logsCh, cleanup
}

func TestServeStartupDetailsEndToEnd(t *testing.T) {
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("dag1").AddTaskWithName("simpleTask", simpleTask)
	})

	done := make(chan error, 1)
	go func() { done <- Serve(bundle, commAddr, logsAddr) }()

	commConn := <-commCh
	defer commConn.Close()
	logsConn := <-logsCh
	defer logsConn.Close()

	payload, err := encodeRequest(0, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":         "550e8400-e29b-41d4-a716-446655440000",
			"dag_id":     "dag1",
			"task_id":    "simpleTask",
			"run_id":     "run1",
			"try_number": 1,
		},
		"bundle_info": map[string]any{"name": "fake", "version": "1.0"},
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, payload))

	frame, err := readFrame(commConn)
	require.NoError(t, err)
	require.True(t, isNilRaw(frame.Err))
	assert.Equal(t, "SucceedTask", peekBodyType(frame.Body))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after task completion")
	}
}

func TestServeUsesSupervisorLogLevelEnvironment(t *testing.T) {
	t.Setenv(loggingLevelEnv, "ERROR")
	t.Setenv(namespaceLevelsEnv, "example=DEBUG")

	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("dag1").AddTaskWithName("logging", func(actx contexttest.Context) error {
			logger := actx.Logger()
			logger.Info("global filtered")
			logger.WithGroup("example.child").Debug("namespace debug")
			logger.WithGroup("unrelated").Warn("unrelated filtered")
			logger.Error("global error")
			return nil
		})
	})

	done := make(chan error, 1)
	go func() { done <- Serve(bundle, commAddr, logsAddr) }()

	commConn := <-commCh
	defer commConn.Close()
	logsConn := <-logsCh
	defer logsConn.Close()
	require.NoError(t, commConn.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, logsConn.SetDeadline(time.Now().Add(10*time.Second)))

	payload, err := encodeRequest(0, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":         "550e8400-e29b-41d4-a716-446655440000",
			"dag_id":     "dag1",
			"task_id":    "logging",
			"run_id":     "run1",
			"try_number": 1,
		},
		"bundle_info": map[string]any{"name": "fake", "version": "1.0"},
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, payload))

	frame, err := readFrame(commConn)
	require.NoError(t, err)
	require.True(t, isNilRaw(frame.Err))
	assert.Equal(t, "SucceedTask", peekBodyType(frame.Body))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after task completion")
	}

	output, err := io.ReadAll(logsConn)
	require.NoError(t, err)
	var events []string
	for line := range strings.Lines(string(output)) {
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		events = append(events, entry["event"].(string))
	}
	assert.Equal(t, []string{"namespace debug", "global error"}, events)
}

// TestServeClientRoundTripEndToEnd drives a task that calls back into the
// supervisor mid-execution, so the comm dispatcher's request/response
// multiplexing is exercised against the real Serve rather than only the
// no-op task path. The registered task pulls a variable (GetVariable) and
// returns a value (which triggers a return-value SetXCom push); the fake
// supervisor must answer both runtime-initiated requests before the terminal
// SucceedTask frame is sent.
func TestServeClientRoundTripEndToEnd(t *testing.T) {
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	// Unique key so the GetVariable env-var fast path
	// (AIRFLOW_VAR_<KEY>) cannot short-circuit the socket round trip.
	const varKey = "go_sdk_round_trip_only_key"

	var gotVar string
	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("dag1").AddTaskWithName("getvar",
			func(actx contexttest.Context) (string, error) {
				v, err := actx.Client().GetVariable(actx, varKey)
				if err != nil {
					return "", err
				}
				gotVar = v
				return "xval", nil
			})
	})

	done := make(chan error, 1)
	go func() { done <- Serve(bundle, commAddr, logsAddr) }()

	commConn := <-commCh
	defer commConn.Close()
	logsConn := <-logsCh
	defer logsConn.Close()

	// Bound every read/write so a regression (e.g. the env-var fast path
	// swallowing the request, or a dispatcher deadlock) fails fast instead of
	// hanging until the Go test timeout.
	require.NoError(t, commConn.SetDeadline(time.Now().Add(10*time.Second)))

	// 1. Kick off task execution.
	startup, err := encodeRequest(0, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":         "550e8400-e29b-41d4-a716-446655440000",
			"dag_id":     "dag1",
			"task_id":    "getvar",
			"run_id":     "run1",
			"try_number": 1,
		},
		"bundle_info": map[string]any{"name": "fake", "version": "1.0"},
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, startup))

	// 2. The task's GetVariable call blocks until the supervisor answers.
	varReq, err := readFrame(commConn)
	require.NoError(t, err)
	require.True(t, isNilRaw(varReq.Err))
	varReqBody := rawToMap(t, varReq.Body)
	assert.Equal(t, "GetVariable", varReqBody["type"])
	assert.Equal(t, varKey, varReqBody["key"])

	varReply, err := encodeRequest(varReq.ID, map[string]any{
		"type":  "VariableResult",
		"key":   varKey,
		"value": "hello",
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, varReply))

	// 3. Returning a value triggers a return-value XCom push; answer it with
	//    an empty (non-error) response so PushXCom unblocks.
	xcomReq, err := readFrame(commConn)
	require.NoError(t, err)
	require.True(t, isNilRaw(xcomReq.Err))
	xcomReqBody := rawToMap(t, xcomReq.Body)
	assert.Equal(t, "SetXCom", xcomReqBody["type"])
	assert.Equal(t, "return_value", xcomReqBody["key"])
	assert.Equal(t, "xval", xcomReqBody["value"])
	assert.NotEqual(t, varReq.ID, xcomReq.ID, "second runtime request must use a fresh frame id")

	xcomReply, err := encodeRequest(xcomReq.ID, map[string]any{})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, xcomReply))

	// 4. With both calls answered, the task finishes and Serve ships the
	//    terminal SucceedTask frame on the StartupDetails frame id.
	term, err := readFrame(commConn)
	require.NoError(t, err)
	require.True(t, isNilRaw(term.Err))
	assert.Equal(t, "SucceedTask", peekBodyType(term.Body))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after task completion")
	}

	assert.Equal(t, "hello", gotVar)
}

// TestServeSkipsDownstreamTasksEndToEnd drives a task that skips downstream tasks through the
// real Serve. Before the terminal SucceedTask frame, the supervisor gets the return value XCom.
// If there is a task to skip, the skipmixin_key XCom with its task_id and the SkipDownstreamTasks
// request follow, in that order.
func TestServeSkipsDownstreamTasksEndToEnd(t *testing.T) {
	xcom := func(key string, value any) map[string]any {
		return map[string]any{
			"type":    "SetXCom",
			"dag_id":  "dag1",
			"run_id":  "run1",
			"task_id": "decide",
			"key":     key,
			"value":   value,
		}
	}
	tests := []struct {
		name         string
		result       bool
		wantRequests []map[string]any
		// wantSkipLogs holds the task_ids of each "Skipping downstream tasks" log entry.
		wantSkipLogs []any
	}{
		{
			name:   "false skips load",
			result: false,
			wantRequests: []map[string]any{
				xcom("return_value", false),
				xcom("skipmixin_key", map[string]any{"skipped": []any{"load"}}),
				{"type": "SkipDownstreamTasks", "tasks": []any{"load"}},
			},
			wantSkipLogs: []any{[]any{"load"}},
		},
		{
			name:   "true skips nothing",
			result: true,
			wantRequests: []map[string]any{
				xcom("return_value", true),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
			defer cleanup()

			decide, err := bundle.NewPositionalBranchFunction(
				func(contexttest.Context) (bool, error) { return tt.result, nil },
				func(result any) (any, []string, error) {
					if result.(bool) {
						return result, nil, nil
					}
					return result, []string{"load"}, nil
				},
			)
			require.NoError(t, err)
			tasks := testBundle{"dag1": testDag{"decide": decide}}

			done := make(chan error, 1)
			go func() { done <- Serve(tasks, commAddr, logsAddr) }()

			commConn := <-commCh
			defer commConn.Close()
			logsConn := <-logsCh
			defer logsConn.Close()
			require.NoError(t, commConn.SetDeadline(time.Now().Add(10*time.Second)))
			require.NoError(t, logsConn.SetDeadline(time.Now().Add(10*time.Second)))

			startup, err := encodeRequest(0, map[string]any{
				"type": "StartupDetails",
				"ti": map[string]any{
					"id":         "550e8400-e29b-41d4-a716-446655440000",
					"dag_id":     "dag1",
					"task_id":    "decide",
					"run_id":     "run1",
					"try_number": 1,
				},
				"bundle_info": map[string]any{"name": "fake", "version": "1.0"},
			})
			require.NoError(t, err)
			require.NoError(t, writeFrame(commConn, startup))

			// Until the terminal frame arrives, answer each runtime request with an empty response
			// so that the task goes on.
			var requests []map[string]any
			for {
				frame, err := readFrame(commConn)
				require.NoError(t, err)
				require.True(t, isNilRaw(frame.Err))
				if peekBodyType(frame.Body) == "SucceedTask" {
					break
				}
				requests = append(requests, rawToMap(t, frame.Body))
				reply, err := encodeRequest(frame.ID, map[string]any{})
				require.NoError(t, err)
				require.NoError(t, writeFrame(commConn, reply))
			}
			assert.Equal(t, tt.wantRequests, requests)

			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("Serve did not return after task completion")
			}

			output, err := io.ReadAll(logsConn)
			require.NoError(t, err)
			var skipLogs []any
			for line := range strings.Lines(string(output)) {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
				if entry["event"] == "Skipping downstream tasks" {
					skipLogs = append(skipLogs, entry["task_ids"])
				}
			}
			assert.Equal(t, tt.wantSkipLogs, skipLogs)
		})
	}
}

// serveTask runs task as the task instance taskID of dag1 through the real Serve. The
// StartupDetails frame carries mapIndex, unless it is nil, and tiContext as ti_context. The fake
// supervisor passes each runtime request to answer, which returns the ErrorResponse to reply with,
// or nil to reply with an empty response. serveTask returns the requests in the order they
// arrived, and the body of the terminal frame.
func serveTask(
	t *testing.T,
	taskID string,
	task bundle.Task,
	mapIndex *int,
	tiContext map[string]any,
	answer func(request map[string]any) map[string]any,
) (requests []map[string]any, terminal map[string]any) {
	t.Helper()
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	done := make(chan error, 1)
	go func() { done <- Serve(testBundle{"dag1": testDag{taskID: task}}, commAddr, logsAddr) }()

	commConn := <-commCh
	defer commConn.Close()
	logsConn := <-logsCh
	defer logsConn.Close()
	go func() { _, _ = io.Copy(io.Discard, logsConn) }()
	require.NoError(t, commConn.SetDeadline(time.Now().Add(10*time.Second)))

	ti := map[string]any{
		"id":         "550e8400-e29b-41d4-a716-446655440000",
		"dag_id":     "dag1",
		"task_id":    taskID,
		"run_id":     "run1",
		"try_number": 2,
	}
	if mapIndex != nil {
		ti["map_index"] = *mapIndex
	}
	startup, err := encodeRequest(0, map[string]any{
		"type":        "StartupDetails",
		"ti":          ti,
		"ti_context":  tiContext,
		"bundle_info": map[string]any{"name": "fake", "version": "1.0"},
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, startup))

	for {
		frame, err := readFrame(commConn)
		require.NoError(t, err)
		require.True(t, isNilRaw(frame.Err))
		body := rawToMap(t, frame.Body)
		switch body["type"] {
		case "SucceedTask", "TaskState", "RetryTask":
			terminal = body
		}
		if terminal != nil {
			break
		}
		requests = append(requests, body)
		var reply []byte
		if errBody := answer(body); errBody != nil {
			reply = encodeResponseFrame(t, frame.ID, nil, errBody)
		} else {
			reply, err = encodeRequest(frame.ID, map[string]any{})
			require.NoError(t, err)
		}
		require.NoError(t, writeFrame(commConn, reply))
	}

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after task completion")
	}
	return requests, terminal
}

// answerAll replies to every runtime request with an empty response.
func answerAll(map[string]any) map[string]any { return nil }

// TestServeClearsTheXComsOfEarlierTriesEndToEnd pins that the runtime deletes each XCom that
// ti_context.xcom_keys_to_clear lists before the task sends anything. The DeleteXCom frame leaves
// map_index out for an unmapped task instance and carries the index of a mapped one, 0 included.
func TestServeClearsTheXComsOfEarlierTriesEndToEnd(t *testing.T) {
	deleteXCom := func(key string, mapIndex any) map[string]any {
		frame := map[string]any{
			"type":    "DeleteXCom",
			"dag_id":  "dag1",
			"run_id":  "run1",
			"task_id": "extract",
			"key":     key,
		}
		if mapIndex != nil {
			frame["map_index"] = mapIndex
		}
		return frame
	}
	returnValue := func(mapIndex any) map[string]any {
		frame := map[string]any{
			"type":    "SetXCom",
			"dag_id":  "dag1",
			"run_id":  "run1",
			"task_id": "extract",
			"key":     "return_value",
			"value":   "rows",
		}
		if mapIndex != nil {
			frame["map_index"] = mapIndex
		}
		return frame
	}
	tests := []struct {
		name         string
		mapIndex     *int
		keys         []any
		wantRequests []map[string]any
	}{
		{
			name:     "unmapped",
			mapIndex: ptr(-1),
			keys:     []any{"return_value", "skipmixin_key"},
			wantRequests: []map[string]any{
				deleteXCom("return_value", nil),
				deleteXCom("skipmixin_key", nil),
				returnValue(nil),
			},
		},
		{
			name:     "map index 0",
			mapIndex: ptr(0),
			keys:     []any{"return_value"},
			wantRequests: []map[string]any{
				deleteXCom("return_value", int8(0)),
				returnValue(int8(0)),
			},
		},
		{
			name:     "map index 2",
			mapIndex: ptr(2),
			keys:     []any{"return_value"},
			wantRequests: []map[string]any{
				deleteXCom("return_value", int8(2)),
				returnValue(int8(2)),
			},
		},
		{
			name:         "no keys",
			mapIndex:     ptr(-1),
			wantRequests: []map[string]any{returnValue(nil)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			extract, err := bundle.NewTaskFunction(
				func(contexttest.Context) (string, error) { return "rows", nil },
			)
			require.NoError(t, err)
			tiContext := map[string]any{}
			if tt.keys != nil {
				tiContext["xcom_keys_to_clear"] = tt.keys
			}

			requests, terminal := serveTask(
				t,
				"extract",
				extract,
				tt.mapIndex,
				tiContext,
				answerAll,
			)

			assert.Equal(t, tt.wantRequests, requests)
			assert.Equal(t, "SucceedTask", terminal["type"])
		})
	}
}

// TestServeFailsWhenItCannotClearAnXComEndToEnd pins that the task does not run when the runtime
// cannot delete an XCom that ti_context.xcom_keys_to_clear lists. The task then ends like a failed
// task: as RetryTask when ti_context.should_retry is set, and as a FAILED TaskState otherwise.
func TestServeFailsWhenItCannotClearAnXComEndToEnd(t *testing.T) {
	for _, shouldRetry := range []bool{true, false} {
		t.Run(fmt.Sprintf("should_retry=%t", shouldRetry), func(t *testing.T) {
			ran := false
			extract, err := bundle.NewTaskFunction(func(contexttest.Context) error {
				ran = true
				return nil
			})
			require.NoError(t, err)

			requests, terminal := serveTask(t, "extract", extract, nil, map[string]any{
				"xcom_keys_to_clear": []any{"return_value", "skipmixin_key"},
				"should_retry":       shouldRetry,
			}, func(map[string]any) map[string]any {
				return map[string]any{
					"type":   "ErrorResponse",
					"error":  "API_SERVER_ERROR",
					"detail": map[string]any{"status_code": 500},
				}
			})

			assert.False(t, ran, "the task must not run")
			assert.Equal(t, []map[string]any{{
				"type":    "DeleteXCom",
				"dag_id":  "dag1",
				"run_id":  "run1",
				"task_id": "extract",
				"key":     "return_value",
			}}, requests, "the runtime stops at the first XCom it cannot delete")
			if shouldRetry {
				assert.Equal(t, "RetryTask", terminal["type"])
				assert.Contains(t, terminal["retry_reason"], "API_SERVER_ERROR")
			} else {
				assert.Equal(t, "TaskState", terminal["type"])
				assert.Equal(t, "failed", terminal["state"])
			}
		})
	}
}

// TestServeClearsXComsBeforeItBindsArgumentsEndToEnd pins that the runtime deletes the XComs
// before it converts arg_bindings. When arg_bindings is invalid, the task fails without running,
// and the XComs are still gone, as they are for a Python task whose templates fail to render.
func TestServeClearsXComsBeforeItBindsArgumentsEndToEnd(t *testing.T) {
	ran := false
	extract, err := bundle.NewTaskFunction(func(contexttest.Context) error {
		ran = true
		return nil
	})
	require.NoError(t, err)

	requests, terminal := serveTask(t, "extract", extract, nil, map[string]any{
		"xcom_keys_to_clear": []any{"skipmixin_key"},
		"arg_bindings":       []any{map[string]any{"kind": "bogus", "name": "x"}},
	}, answerAll)

	assert.False(t, ran, "the task must not run")
	assert.Equal(t, []map[string]any{{
		"type":    "DeleteXCom",
		"dag_id":  "dag1",
		"run_id":  "run1",
		"task_id": "extract",
		"key":     "skipmixin_key",
	}}, requests)
	assert.Equal(t, "TaskState", terminal["type"])
	assert.Equal(t, "failed", terminal["state"])
}

// TestServeConditionDoesNotLeaveTheListOfAnEarlierTryEndToEnd covers a task that skips downstream
// tasks and whose earlier try skipped load. When this try fails or skips nothing, it writes no
// list, so only the deletion keeps NotPreviouslySkippedDep from reading the old list and skipping
// load again. The fake supervisor keeps the XComs of the task instance to show what is left.
func TestServeConditionDoesNotLeaveTheListOfAnEarlierTryEndToEnd(t *testing.T) {
	tests := []struct {
		name         string
		fn           func(contexttest.Context) (bool, error)
		wantTerminal string
		wantXComs    map[string]any
	}{
		{
			name: "fails",
			fn: func(contexttest.Context) (bool, error) {
				return false, errors.New("cannot reach the table")
			},
			wantTerminal: "TaskState",
			wantXComs:    map[string]any{},
		},
		{
			name:         "skips nothing",
			fn:           func(contexttest.Context) (bool, error) { return true, nil },
			wantTerminal: "SucceedTask",
			wantXComs:    map[string]any{"return_value": true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decide, err := bundle.NewPositionalBranchFunction(
				tt.fn,
				func(result any) (any, []string, error) {
					if result.(bool) {
						return result, nil, nil
					}
					return result, []string{"load"}, nil
				},
			)
			require.NoError(t, err)

			xcoms := map[string]any{
				"return_value":  false,
				"skipmixin_key": map[string]any{"skipped": []any{"load"}},
			}
			requests, terminal := serveTask(t, "decide", decide, nil, map[string]any{
				"xcom_keys_to_clear": []any{"return_value", "skipmixin_key"},
			}, func(request map[string]any) map[string]any {
				switch request["type"] {
				case "DeleteXCom":
					delete(xcoms, request["key"].(string))
				case "SetXCom":
					xcoms[request["key"].(string)] = request["value"]
				}
				return nil
			})

			require.GreaterOrEqual(t, len(requests), 2)
			assert.Equal(t, "DeleteXCom", requests[0]["type"])
			assert.Equal(t, "DeleteXCom", requests[1]["type"])
			for _, request := range requests {
				assert.NotEqual(t, "SkipDownstreamTasks", request["type"])
			}
			assert.Equal(t, tt.wantXComs, xcoms)
			assert.Equal(t, tt.wantTerminal, terminal["type"])
		})
	}
}

// TestServeFailureAfterConnectClosesComm asserts the failure-signaling
// contract: when Serve fails after the sockets are connected, it returns the
// error (so the caller exits non-zero) without writing a terminal frame. The
// supervisor observes the failure as the comm socket closing rather than as a
// TaskState message.
func TestServeFailureAfterConnectClosesComm(t *testing.T) {
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	bundle := buildBundle(t, func(r testBundle) {
		r.AddDag("dag1").AddTaskWithName("simpleTask", simpleTask)
	})

	done := make(chan error, 1)
	go func() { done <- Serve(bundle, commAddr, logsAddr) }()

	commConn := <-commCh
	defer commConn.Close()
	logsConn := <-logsCh
	defer logsConn.Close()

	// Serve expects StartupDetails or DagFileParseRequest as the first frame, so it fails to
	// decode a VariableResult.
	payload, err := encodeRequest(
		0,
		map[string]any{"type": "VariableResult", "key": "k", "value": "v"},
	)
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, payload))

	select {
	case err := <-done:
		require.ErrorContains(t, err, "decoding initial message")
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after a first frame it cannot decode")
	}

	// No terminal frame was sent: the next read on the comm socket sees the
	// connection close instead of a decodable frame.
	require.NoError(t, commConn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err = readFrame(commConn)
	require.Error(t, err)
}

// parseBundle is a bundle that serializes Dags and has no task to run.
type parseBundle struct {
	testBundle
	*serializedDags
}

// startDagParse runs Serve for dags and sends it a DagFileParseRequest. It returns the comm and
// logs connections, the frame that Serve answers with, and the channel that gets what Serve
// returns.
func startDagParse(
	t *testing.T,
	dags *serializedDags,
) (commConn, logsConn net.Conn, frame IncomingFrame, done <-chan error) {
	t.Helper()
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	t.Cleanup(cleanup)

	served := make(chan error, 1)
	go func() { served <- Serve(parseBundle{testBundle{}, dags}, commAddr, logsAddr) }()

	commConn = <-commCh
	t.Cleanup(func() { commConn.Close() })
	logsConn = <-logsCh
	t.Cleanup(func() { logsConn.Close() })
	deadline := time.Now().Add(10 * time.Second)
	require.NoError(t, commConn.SetDeadline(deadline))
	require.NoError(t, logsConn.SetDeadline(deadline))

	payload, err := encodeRequest(0, map[string]any{
		"type":        "DagFileParseRequest",
		"file":        "/bundles/go/etl",
		"bundle_path": "/bundles/go",
		"bundle_name": "go",
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, payload))

	frame, err = readFrame(commConn)
	require.NoError(t, err)
	require.True(t, isNilRaw(frame.Err))
	return commConn, logsConn, frame, served
}

func TestServeDagFileParseRequestEndToEnd(t *testing.T) {
	dags := &serializedDags{dags: []bundle.SerializedDag{serializedDag("etl")}}
	commConn, _, frame, done := startDagParse(t, dags)

	var result genmodels.DagFileParsingResult
	require.NoError(t, decodeBody(frame.Body, &result))
	assert.Equal(t, "DagFileParsingResult", result.Type)
	assert.Equal(t, "/bundles/go/etl", result.Fileloc)
	require.Len(t, result.SerializedDags, 1)
	assert.Equal(t, "etl", result.SerializedDags[0].Data["dag"].(map[string]any)["dag_id"])
	assert.Equal(t, "etl", dags.relative)
	require.NoError(t, writeFrame(commConn, encodeResponseFrame(t, frame.ID, nil, nil)))

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the Dag processor acknowledged the result")
	}
}

func TestServeFailsWhenTheDagProcessorRejectsTheParseResult(t *testing.T) {
	commConn, logsConn, frame, done := startDagParse(t, &serializedDags{})

	rejection := map[string]any{
		"type":   "ErrorResponse",
		"error":  "generic_error",
		"detail": map[string]any{"message": "A parse result was already received"},
	}
	require.NoError(t, writeFrame(commConn, encodeResponseFrame(t, frame.ID, nil, rejection)))

	select {
	case err := <-done:
		require.ErrorContains(t, err, "sending Dag parsing result")
		require.ErrorContains(t, err, "A parse result was already received")
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after the Dag processor rejected the result")
	}

	// main's log.Fatal writes to the logs socket after Serve has closed that socket. The error
	// therefore shows up only if Serve logs it first.
	logs, err := io.ReadAll(logsConn)
	require.NoError(t, err)
	var failure map[string]any
	for line := range bytes.Lines(logs) {
		var record map[string]any
		require.NoError(t, json.Unmarshal(line, &record))
		if record["event"] == "Failed to send the Dag parsing result" {
			failure = record
		}
	}
	require.NotNil(t, failure, "Serve did not log why it failed: %s", logs)
	assert.Equal(t, "error", failure["level"])
	assert.Contains(t, failure["error"], "A parse result was already received")
}
