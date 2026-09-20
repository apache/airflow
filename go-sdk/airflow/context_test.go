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
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/sdk"
)

type probeKey struct{}

// fakeClient satisfies sdk.Client without implementing any of it.
// These tests only check which value the accessor hands back.
type fakeClient struct {
	sdk.Client
}

func testValues() (*slog.Logger, *fakeClient, TaskInstance, DagRun) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := &fakeClient{}
	ti := TaskInstance{DagID: "py_etl", RunID: "run1", TaskID: "transform", TryNumber: 2}
	dagRun := DagRun{DagID: "py_etl", RunID: "run1"}
	return logger, client, ti, dagRun
}

func TestNewContextAccessors(t *testing.T) {
	logger, client, ti, dagRun := testValues()

	base := context.WithValue(context.Background(), probeKey{}, "probe-value")
	actx := NewContext(base, logger, client, ti, dagRun)

	assert.Same(t, logger, actx.Logger())
	assert.Same(t, client, actx.Client())
	assert.Equal(t, ti, actx.TaskInstance())
	assert.Equal(t, dagRun, actx.DagRun())
	assert.Equal(
		t,
		"probe-value",
		actx.Value(probeKey{}),
		"context behaviour must delegate to the base context",
	)
}

// Fail at construction rather than hand out a Context whose accessors return nil.
func TestNewContextRejectsNilArgs(t *testing.T) {
	logger, client, ti, dagRun := testValues()

	// Keyed by the expected panic value: context.WithValue panics on a nil ctx
	// by itself, so asserting the value is what pins the check to ours.
	cases := map[string]func(){
		"airflow.NewContext: nil context.Context": func() {
			NewContext(nil, logger, client, ti, dagRun)
		},
		"airflow.NewContext: nil logger": func() {
			NewContext(context.Background(), nil, client, ti, dagRun)
		},
		"airflow.NewContext: nil client": func() {
			NewContext(context.Background(), logger, nil, ti, dagRun)
		},
	}
	for want, build := range cases {
		t.Run(want, func(t *testing.T) {
			assert.PanicsWithValue(t, want, build)
		})
	}
}

// execution.Serve traps the supervisor's SIGINT/SIGTERM into the context
// the runtime binds, so a cooperative handler sees it on actx.Done().
func TestContextDoneFollowsBaseCancellation(t *testing.T) {
	logger, client, ti, dagRun := testValues()

	base, cancel := context.WithCancel(context.Background())
	actx := NewContext(base, logger, client, ti, dagRun)

	select {
	case <-actx.Done():
		t.Fatal("actx must not be done before the base context is cancelled")
	default:
	}

	cancel()

	select {
	case <-actx.Done():
	case <-time.After(time.Second):
		t.Fatal("actx.Done() must fire when the base context is cancelled")
	}
	assert.ErrorIs(t, actx.Err(), context.Canceled)
}

func TestFromContextOnTaskContext(t *testing.T) {
	logger, client, ti, dagRun := testValues()
	actx := NewContext(context.Background(), logger, client, ti, dagRun)

	got, ok := FromContext(actx)
	require.True(t, ok, "the Context a handler is given must carry itself")
	assert.Same(t, logger, got.Logger())
	assert.Same(t, client, got.Client())
	assert.Equal(t, ti, got.TaskInstance())
	assert.Equal(t, dagRun, got.DagRun())
}

// A helper recovers the surface whatever the handler wrapped the context in.
func TestFromContextOnDerivedContext(t *testing.T) {
	logger, client, ti, dagRun := testValues()
	actx := NewContext(context.Background(), logger, client, ti, dagRun)

	cases := map[string]context.Context{
		"plain context.Context": context.Context(actx),
		"context.WithValue":     context.WithValue(actx, probeKey{}, "probe-value"),
		"context.WithoutCancel": context.WithoutCancel(actx),
	}
	for name, ctx := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := FromContext(ctx)
			require.True(t, ok)
			assert.Same(t, logger, got.Logger())
			assert.Same(t, client, got.Client())
			assert.Equal(t, ti, got.TaskInstance())
			assert.Equal(t, dagRun, got.DagRun())
		})
	}
}

// The recovered Context keeps the caller's context, so a cancellation added
// on the way down applies to it.
func TestFromContextKeepsCallerCancellation(t *testing.T) {
	logger, client, ti, dagRun := testValues()
	actx := NewContext(context.Background(), logger, client, ti, dagRun)

	inner, cancel := context.WithCancel(actx)
	got, ok := FromContext(inner)
	require.True(t, ok)

	cancel()

	select {
	case <-got.Done():
	case <-time.After(time.Second):
		t.Fatal("the recovered Context must honour the caller's cancellation")
	}
	assert.NoError(t, actx.Err(), "cancelling a derived context must not cancel the task")
}

// A context that never passed through NewContext must report false
// rather than hand back a Context that looks usable.
func TestFromContextOnPlainContext(t *testing.T) {
	got, ok := FromContext(context.Background())
	assert.False(t, ok)
	assert.Equal(t, Context{}, got)

	got, ok = FromContext(nil)
	assert.False(t, ok)
	assert.Equal(t, Context{}, got)
}
