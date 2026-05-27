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
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
)

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

func TestDagParsing(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		d := r.AddDag("test_dag")
		d.AddTask(simpleTask)
	})

	req := &DagFileParseRequest{
		File:       "/bundles/test/main.go",
		BundlePath: "/bundles/test",
	}

	result := ParseDags(bundle, req)

	assert.Equal(t, "DagFileParsingResult", result["type"])
	assert.Equal(t, "/bundles/test/main.go", result["fileloc"])

	serializedDags, ok := result["serialized_dags"].([]any)
	require.True(t, ok)
	require.Len(t, serializedDags, 1)

	dagEntry := serializedDags[0].(map[string]any)
	data := dagEntry["data"].(map[string]any)
	assert.Equal(t, 3, data["__version"])

	dagMap := data["dag"].(map[string]any)
	assert.Equal(t, "test_dag", dagMap["dag_id"])

	tt := dagMap["timetable"].(map[string]any)
	assert.Equal(t, "airflow.timetables.simple.NullTimetable", tt["__type"])

	tasks := dagMap["tasks"].([]any)
	require.Len(t, tasks, 1)
	taskMap := tasks[0].(map[string]any)
	assert.Equal(t, "operator", taskMap["__type"])
	taskData := taskMap["__var"].(map[string]any)
	assert.Equal(t, "simpleTask", taskData["task_id"])
	assert.Equal(t, "go", taskData["language"])
}

func TestDagParsingMultipleDagsPreservesOrder(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("dag1").AddTask(simpleTask)
		r.AddDag("dag2").AddTask(failingTask)
	})

	req := &DagFileParseRequest{File: "/bundle/main.go", BundlePath: "/bundle"}
	result := ParseDags(bundle, req)

	serializedDags := result["serialized_dags"].([]any)
	require.Len(t, serializedDags, 2)

	dag1Data := serializedDags[0].(map[string]any)["data"].(map[string]any)["dag"].(map[string]any)
	assert.Equal(t, "dag1", dag1Data["dag_id"])

	dag2Data := serializedDags[1].(map[string]any)["data"].(map[string]any)["dag"].(map[string]any)
	assert.Equal(t, "dag2", dag2Data["dag_id"])
}

func TestTaskRunnerSuccess(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(simpleTask)
	})

	details := &StartupDetails{
		TI: TaskInstanceInfo{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "simpleTask",
			RunID:    "run1",
			MapIndex: -1,
		},
		BundleInfo: BundleInfoMsg{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(bundle, details, comm, logger)
	assert.Equal(t, "SucceedTask", result["type"])
}

func TestTaskRunnerFailure(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(failingTask)
	})

	details := &StartupDetails{
		TI: TaskInstanceInfo{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "failingTask",
			RunID:    "run1",
			MapIndex: -1,
		},
		BundleInfo: BundleInfoMsg{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(bundle, details, comm, logger)
	assert.Equal(t, "TaskState", result["type"])
	assert.Equal(t, "failed", result["state"])
}

func TestTaskRunnerTaskNotFound(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(simpleTask)
	})

	details := &StartupDetails{
		TI: TaskInstanceInfo{
			ID:     "550e8400-e29b-41d4-a716-446655440000",
			DagID:  "test_dag",
			TaskID: "nonexistent",
			RunID:  "run1",
		},
		BundleInfo: BundleInfoMsg{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(bundle, details, comm, logger)
	assert.Equal(t, "TaskState", result["type"])
	assert.Equal(t, "removed", result["state"])
}

func TestTaskRunnerPanic(t *testing.T) {
	bundle := buildBundle(t, func(r bundlev1.Registry) {
		r.AddDag("test_dag").AddTask(panicTask)
	})

	details := &StartupDetails{
		TI: TaskInstanceInfo{
			ID:       "550e8400-e29b-41d4-a716-446655440000",
			DagID:    "test_dag",
			TaskID:   "panicTask",
			RunID:    "run1",
			MapIndex: -1,
		},
		BundleInfo: BundleInfoMsg{Name: "test", Version: "1.0"},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), io.Discard, logger)

	result := RunTask(bundle, details, comm, logger)
	assert.Equal(t, "TaskState", result["type"])
	assert.Equal(t, "failed", result["state"])
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

func TestServeDagFileParseEndToEnd(t *testing.T) {
	commAddr, logsAddr, commCh, logsCh, cleanup := startSupervisor(t)
	defer cleanup()

	provider := &fakeProvider{
		register: func(r bundlev1.Registry) error {
			d := r.AddDag("simple_dag")
			d.AddTask(simpleTask)
			return nil
		},
	}

	done := make(chan error, 1)
	go func() { done <- Serve(provider, commAddr, logsAddr) }()

	commConn := <-commCh
	require.NotNil(t, commConn)
	defer commConn.Close()
	logsConn := <-logsCh
	require.NotNil(t, logsConn)
	defer logsConn.Close()

	// Send DagFileParseRequest as a request frame.
	payload, err := encodeRequest(0, map[string]any{
		"type":        "DagFileParseRequest",
		"file":        "/bundle/main.go",
		"bundle_path": "/bundle",
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(commConn, payload))

	frame, err := readFrame(commConn)
	require.NoError(t, err)
	assert.Equal(t, 0, frame.ID)
	require.Nil(t, frame.Err)
	assert.Equal(t, "DagFileParsingResult", frame.Body["type"])

	dags := frame.Body["serialized_dags"].([]any)
	require.Len(t, dags, 1)
	dag := dags[0].(map[string]any)["data"].(map[string]any)["dag"].(map[string]any)
	assert.Equal(t, "simple_dag", dag["dag_id"])

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after parse result")
	}
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
	require.Nil(t, frame.Err)
	assert.Equal(t, "SucceedTask", frame.Body["type"])

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after task completion")
	}
}
