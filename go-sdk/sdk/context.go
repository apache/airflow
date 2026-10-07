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

package sdk

import (
	"context"
	"time"
)

// TIRunContext is a context.Context that also exposes the identifiers and
// scheduling timestamps of the task instance that is executing, along with the
// Dag run it belongs to.
//
// The runtime uses it to carry those values on the task's context. Task
// functions read them from the [github.com/apache/airflow/go-sdk/airflow.Context]
// they take first, which exposes the same TaskInstance and DagRun next to the
// logger and the client.
//
// It is an interface, and only this package implements it.
//
// The context package's advice against storing a Context in a struct
// (https://pkg.go.dev/context#hdr-Contexts_and_structs) is about domain types that would
// carry a request-scoped context in a field, not about a purpose-built context type.
// That is why [github.com/apache/airflow/go-sdk/airflow.Context], the value a task handler
// takes first, is a struct embedding context.Context.
type TIRunContext interface {
	context.Context

	// TaskInstance identifies the task instance that is executing.
	TaskInstance() TaskInstance
	// DagRun identifies the Dag run the task instance belongs to.
	DagRun() DagRun
}

// NewTIRunContext returns a TIRunContext that delegates context behaviour to
// ctx and exposes ti and dagRun. It panics on a nil ctx, mirroring the context
// package's own constructors. The runtime calls it to record the task instance
// and Dag run on the task's context.
func NewTIRunContext(ctx context.Context, ti TaskInstance, dagRun DagRun) TIRunContext {
	if ctx == nil {
		// This cannot happen from the runtime, which always passes a non-nil
		// base context. A nil ctx is a programming error in the caller, so
		// fail loudly instead of masking it.
		panic("sdk.NewTIRunContext: cannot create TIRunContext from nil context.Context")
	}
	return tiRunContext{Context: ctx, ti: ti, dagRun: dagRun}
}

// tiRunContext is the runtime implementation of TIRunContext.
type tiRunContext struct {
	context.Context

	ti     TaskInstance
	dagRun DagRun
}

func (c tiRunContext) TaskInstance() TaskInstance { return c.ti }

func (c tiRunContext) DagRun() DagRun { return c.dagRun }

// TaskInstance identifies the currently executing task instance.
type TaskInstance struct {
	DagID  string
	RunID  string
	TaskID string
	// MapIndex is the index within a dynamically mapped task, or nil for an
	// unmapped (regular) task instance.
	MapIndex  *int
	TryNumber int
}

// DagRun identifies the Dag run the current task instance belongs to and
// carries its scheduling timestamps. The *time.Time fields are nil when the
// supervisor did not provide a value (for example, a manually triggered run
// without a logical date).
type DagRun struct {
	DagID             string
	RunID             string
	LogicalDate       *time.Time
	DataIntervalStart *time.Time
	DataIntervalEnd   *time.Time
}
