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
	"log/slog"

	"github.com/apache/airflow/go-sdk/sdk"
)

type (
	// TaskInstance identifies the task instance a handler is running for.
	TaskInstance = sdk.TaskInstance

	// DagRun identifies the Dag run the task instance belongs to,
	// and carries its scheduling timestamps.
	DagRun = sdk.DagRun
)

// Context is the first parameter of every task handler.
// It is a context.Context bound to the running task, so actx.Done() fires when the supervisor
// asks the task to stop, and it exposes what Airflow gives the task: [Context.Logger],
// [Context.Client], [Context.TaskInstance] and [Context.DagRun].
//
// [NewContext] builds one. The zero Context is not usable.
type Context struct {
	context.Context

	values taskValues
}

// taskValues is what NewContext stores on the context chain so FromContext can recover it.
type taskValues struct {
	logger *slog.Logger
	client sdk.Client
	ti     TaskInstance
	dagRun DagRun
}

type contextKey struct{}

// NewContext returns a [Context] backed by ctx.
// It panics if ctx, logger or client is nil, so every accessor on the returned Context is safe.
//
// The runtime calls it when it binds a handler's first parameter.
// Call it directly to unit-test a handler:
//
//	actx := airflow.NewContext(
//		t.Context(), slog.Default(), fakeClient,
//		airflow.TaskInstance{DagID: "py_etl", TaskID: "transform", TryNumber: 1},
//		airflow.DagRun{DagID: "py_etl", RunID: "run1"},
//	)
//	require.NoError(t, transform(actx, "US"))
func NewContext(
	ctx context.Context,
	logger *slog.Logger,
	client sdk.Client,
	ti TaskInstance,
	dagRun DagRun,
) Context {
	switch {
	case ctx == nil:
		panic("airflow.NewContext: nil context.Context")
	case logger == nil:
		panic("airflow.NewContext: nil logger")
	case client == nil:
		panic("airflow.NewContext: nil client")
	}
	values := taskValues{logger: logger, client: client, ti: ti, dagRun: dagRun}
	return Context{Context: context.WithValue(ctx, contextKey{}, values), values: values}
}

// FromContext recovers the Airflow surface inside a helper typed as a plain context.Context,
// reporting whether ctx carries one.
//
// It succeeds for the [Context] a handler was given and for any context derived from it.
// The returned Context keeps ctx, so a deadline or cancellation added on the way down applies.
func FromContext(ctx context.Context) (Context, bool) {
	if ctx == nil {
		return Context{}, false
	}
	values, ok := ctx.Value(contextKey{}).(taskValues)
	if !ok {
		return Context{}, false
	}
	return Context{Context: ctx, values: values}, true
}

// Logger writes to the task's Airflow log.
// The logger takes a context, so pass the same Context: actx.Logger().InfoContext(actx, "msg").
func (c Context) Logger() *slog.Logger { return c.values.logger }

// Client reads Airflow Variables, Connections and XCom.
// Its calls take a context, so pass the same Context: actx.Client().GetVariable(actx, "name").
func (c Context) Client() sdk.Client { return c.values.client }

// TaskInstance identifies the task instance that is executing.
func (c Context) TaskInstance() TaskInstance { return c.values.ti }

// DagRun identifies the Dag run the task instance belongs to.
func (c Context) DagRun() DagRun { return c.values.dagRun }
