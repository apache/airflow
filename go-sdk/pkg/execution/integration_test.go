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
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
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

func failingTask() error {
	return errors.New("task failed intentionally")
}

func panicTask() error {
	panic("something went wrong")
}

func simpleTask() error {
	return nil
}

// buildBundle wires a bundlev1.Registry from a closure and returns it as a
// bundlev1.Bundle (the materialised registry).
func buildBundle(t *testing.T, register func(bundlev1.Registry)) bundlev1.Bundle {
	t.Helper()
	reg := bundlev1.New()
	register(reg)
	return reg
}

// --- Tests ---

func TestTaskRunnerSuccess(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(simpleTask)
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "simpleTask",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)
}

func TestTaskRunnerFailure(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(failingTask)
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "failingTask",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestTaskRunnerRetry(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(failingTask)
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "failingTask",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
		TIContext: genmodels.TIRunContext{
			ShouldRetry: true,
			MaxTries:    3,
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertRetryTask(t, result, "task failed intentionally")
}

func TestTaskRunnerTaskNotFound(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(simpleTask)
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:     "550e8400-e29b-41d4-a716-446655440000",
			DagID:  "test_dag",
			TaskID: "nonexistent",
			RunID:  "run1",
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateRemoved)
}

func TestTaskRunnerPanic(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(panicTask)
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "panicTask",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestTaskRunnerPanicRetry(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(panicTask)
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "panicTask",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
		TIContext: genmodels.TIRunContext{
			ShouldRetry: true,
			MaxTries:    3,
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertRetryTask(t, result, "panic: something went wrong")
}

// TestTaskRunnerBindsStubArgs covers the TaskFlow path through RunTask: the
// positional-argument spec in ti_context.stub_args binds literals onto the
// task function's data parameters.
func TestTaskRunnerBindsStubArgs(t *testing.T) {
	var gotCountry string
	var gotMeta map[string]any
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(log *slog.Logger, country string, meta map[string]any) error {
				gotCountry = country
				gotMeta = meta
				return nil
			})
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "transform",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
		TIContext: genmodels.TIRunContext{
			StubArgs: &genmodels.StubArgs{
				{Kind: "literal", DataType: "string", Value: "uk"},
				{Kind: "literal", DataType: "object", Value: map[string]any{"k": "v"}},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)
	assert.Equal(t, "uk", gotCountry)
	assert.Equal(t, map[string]any{"k": "v"}, gotMeta)
}

// TestTaskRunnerStubArgsArityMismatch: an argument spec that does not match
// the function's data parameters fails the task loudly instead of running it
// with zero values.
func TestTaskRunnerStubArgsArityMismatch(t *testing.T) {
	ran := false
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(country string, meta map[string]any) error {
				ran = true
				return nil
			})
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "transform",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
		TIContext: genmodels.TIRunContext{
			StubArgs: &genmodels.StubArgs{
				{Kind: "literal", DataType: "string", Value: "uk"},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
	assert.False(t, ran, "the task body must not run on an arity mismatch")
}

// TestTaskRunnerStubArgsTypeMismatch: a declared Dag type that cannot bind to
// the Go parameter type fails the task loudly before the body runs.
func TestTaskRunnerStubArgsTypeMismatch(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTaskWithName("transform",
			func(count int) error { return nil })
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "transform",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
		TIContext: genmodels.TIRunContext{
			StubArgs: &genmodels.StubArgs{
				{Kind: "literal", DataType: "string", Value: "uk"},
			},
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestRunTaskHonorsContextCancellation(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTaskWithName("ctxcheck",
			func(ctx context.Context) error { return ctx.Err() })
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "ctxcheck",
			RunID:    "run1",
			MapIndex: ptr(-1),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	// A cancelled root context must reach the user task through RunTask's
	// threading; the task surfaces ctx.Err(), which RunTask maps to failed.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(ctx, bundle, details, comm, logger)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestRunTaskInjectsRuntimeContext(t *testing.T) {
	logical := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)
	start := logical
	end := logical.Add(time.Hour)

	var got sdk.TIRunContext
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTaskWithName("ctxgrab",
			func(ctx sdk.TIRunContext) error {
				got = ctx
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

	require.NotNil(
		t,
		got,
		"the task must receive a TIRunContext backed by the live task context",
	)
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

func TestRunTaskRuntimeContextMappedIndex(t *testing.T) {
	var got sdk.TIRunContext
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTaskWithName("ctxgrab",
			func(ctx sdk.TIRunContext) error {
				got = ctx
				return nil
			})
	})

	details := &genmodels.StartupDetails{
		TI: genmodels.TaskInstance{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "ctxgrab",
			RunID:    "run1",
			MapIndex: ptr(5),
		},
		BundleInfo: genmodels.BundleInfo{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(context.Background(), bundle, details, comm, logger)
	assertSucceedTask(t, result)

	require.NotNil(t, got.TaskInstance().MapIndex, "a mapped task must surface its index")
	assert.Equal(t, 5, *got.TaskInstance().MapIndex)
}

// --- End-to-end Serve test against a fake supervisor ---

// fakeProvider implements bundlev1.BundleProvider; it lets a test inject the
// registration closure and a synthetic version.
type fakeProvider struct {
	register func(bundlev1.Registry) error
}

func (f *fakeProvider) GetBundleVersion() bundlev1.BundleInfo {
	v := "1.0"
	return bundlev1.BundleInfo{Name: "fake", Version: &v}
}

func (f *fakeProvider) RegisterDags(reg bundlev1.Registry) error {
	if f.register == nil {
		return nil
	}
	return f.register(reg)
}

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

	provider := &fakeProvider{
		register: func(r bundlev1.Registry) error {
			r.AddDag("dag1").AddTask(simpleTask)
			return nil
		},
	}

	done := make(chan error, 1)
	go func() { done <- Serve(provider, commAddr, logsAddr) }()

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
	provider := &fakeProvider{
		register: func(r bundlev1.Registry) error {
			r.AddDag("dag1").AddTaskWithName("getvar",
				func(ctx context.Context, c sdk.Client) (string, error) {
					v, err := c.GetVariable(ctx, varKey)
					if err != nil {
						return "", err
					}
					gotVar = v
					return "xval", nil
				})
			return nil
		},
	}

	done := make(chan error, 1)
	go func() { done <- Serve(provider, commAddr, logsAddr) }()

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

// TestServeRegisterDagsFailureClosesComm asserts the failure-signaling
// contract: when bundle registration fails after the sockets are connected,
// Serve returns the error (so the caller exits non-zero) without writing a
// terminal frame. The supervisor observes the failure as the comm socket
// closing rather than as a TaskState message.
func TestServeRegisterDagsFailureClosesComm(t *testing.T) {
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	wantErr := errors.New("boom registering dags")
	provider := &fakeProvider{
		register: func(bundlev1.Registry) error { return wantErr },
	}

	done := make(chan error, 1)
	go func() { done <- Serve(provider, commAddr, logsAddr) }()

	commConn := <-commCh
	defer commConn.Close()
	logsConn := <-logsCh
	defer logsConn.Close()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.ErrorIs(t, err, wantErr)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after RegisterDags failure")
	}

	// No terminal frame was sent: the next read on the comm socket sees the
	// connection close instead of a decodable frame.
	require.NoError(t, commConn.SetReadDeadline(time.Now().Add(time.Second)))
	_, err := readFrame(commConn)
	require.Error(t, err)
}
