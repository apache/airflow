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
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// BundleRef holds the task handlers and the Dags that this executable registers for Airflow.
// [Bundle] returns an empty one.
type BundleRef struct {
	// closed ends registration for everything the bundle can hold, so a kind added later
	// is covered without a flag of its own. Serve sets it; Register reads it.
	closed atomic.Bool
	// mu is the lock for writes to taskHandlers and dags. Register holds it for the whole call,
	// so two concurrent calls cannot register a task handler and a Dag with the same dag_id.
	// Readers such as LookupTask take only the lock of the map they read.
	mu           sync.Mutex
	taskHandlers taskHandlerMap
	dags         dagMap
}

// Bundle returns an empty bundle. Register the task handlers on it, then call Serve as the
// last statement of main:
//
//	func main() {
//		bundle := airflow.Bundle()
//
//		bundle.Register(
//			airflow.TaskHandler("py_etl", "transform", transform),
//		)
//
//		if err := bundle.Serve(); err != nil {
//			log.Fatal(err)
//		}
//	}
func Bundle() *BundleRef { return &BundleRef{} }

// Registerable is what [BundleRef.Register] accepts: the value [TaskHandler] returns, or the
// [*DagRef] that [Dag] returns.
//
// Its only method is unexported, so a type outside this package cannot declare it.
// A struct that embeds a Registerable still satisfies the interface, and Register panics
// when it is given one.
type Registerable interface{ registerable() }

// Register adds items to the bundle:
//
//	bundle.Register(
//		airflow.TaskHandler("py_etl", "extract", extract),
//		airflow.TaskHandler("py_etl", "transform", transform),
//	)
//
// A package that defines task handlers can export a function that returns them as a slice.
// For example, if package reports exports func Handlers() []airflow.Registerable, main calls:
//
//	bundle.Register(reports.Handlers()...)
//
// Add every task to a Dag before registering the Dag. [DagRef.Task], [DagRef.If],
// [DagRef.Switch], [DagRef.TaskGroup], [IfRef.Then], [IfRef.Else], [SwitchRef.Case] and the
// methods of [TaskGroupRef] panic once the Dag is registered.
//
// Register is where a Dag's task dependencies are checked for a cycle, over the whole graph at
// once: [TaskRef.Before], [TaskRef.After] and [Inputs] each record an edge without walking the
// graph, so building a Dag stays linear in its edges however many a task has. Register also turns
// each edge to or from a task group, which [TaskGroupRef.Before] describes, into edges between
// tasks. It expands those edges one at a time, in the order they were first declared, each
// against every task, every edge declared between two tasks, and the task edges that earlier
// group edges expanded into.
//
// Register panics if a task handler with the same dag_id and task_id is already registered,
// if a Dag with the same dag_id is already registered, if a task handler and a Dag have the
// same dag_id, if the task dependencies of a Dag contain a cycle, or if [BundleRef.Serve] has
// already been called: registration closes when serving starts. A task handler runs a task of a
// Python Dag, so its dag_id cannot also belong to a Dag authored in Go. Register also panics if a
// Dag has a condition from [DagRef.If] without a task from [IfRef.Then], or a switch from
// [DagRef.Switch] without a case from [SwitchRef.Case].
func (b *BundleRef) Register(items ...Registerable) {
	if b.closed.Load() {
		panic(
			"airflow.BundleRef.Register: Serve has already been called; register everything before Serve",
		)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, item := range items {
		switch item := item.(type) {
		case *taskHandler:
			if b.dags.has(item.dagID) {
				panic(fmt.Sprintf(
					"airflow.BundleRef.Register: Dag %q is already registered as a Dag from "+
						"airflow.Dag, so it cannot also have task handlers from "+
						"airflow.TaskHandler",
					item.dagID,
				))
			}
			b.taskHandlers.add(item.dagID, item.taskID, item.task)
		case *DagRef:
			if item == nil {
				panic("airflow.BundleRef.Register: cannot register a nil *airflow.DagRef")
			}
			if b.taskHandlers.hasDag(item.dagID) {
				panic(fmt.Sprintf(
					"airflow.BundleRef.Register: Dag %q already has task handlers from "+
						"airflow.TaskHandler, so it cannot also be registered as a Dag from "+
						"airflow.Dag",
					item.dagID,
				))
			}
			b.dags.add(item)
		default:
			// Either a nil item, or a struct from another package that embeds a Registerable.
			panic(fmt.Sprintf("airflow.BundleRef.Register: cannot register %T", item))
		}
	}
}

// taskHandlerMap holds the registered task handlers by dag_id and task_id.
// It also keeps registration order. The --airflow-metadata manifest lists the tasks of each Dag
// in that order.
type taskHandlerMap struct {
	mu       sync.RWMutex
	handlers map[string]map[string]bundle.Task
	order    []bundle.TaskHandlerInfo
}

var (
	_ bundle.Bundle           = (*taskHandlerMap)(nil)
	_ bundle.EnumerableBundle = (*taskHandlerMap)(nil)
)

func (m *taskHandlerMap) add(dagID, taskID string, task bundle.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.handlers == nil {
		m.handlers = make(map[string]map[string]bundle.Task)
	}
	dagHandlers, exists := m.handlers[dagID]
	if !exists {
		dagHandlers = make(map[string]bundle.Task)
		m.handlers[dagID] = dagHandlers
	}
	if _, exists := dagHandlers[taskID]; exists {
		panic(fmt.Sprintf(
			"airflow.BundleRef.Register: task %q of Dag %q is already registered", taskID, dagID,
		))
	}
	dagHandlers[taskID] = task
	m.order = append(m.order, bundle.TaskHandlerInfo{DagID: dagID, TaskID: taskID})
}

func (m *taskHandlerMap) hasDag(dagID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.handlers[dagID]
	return exists
}

func (m *taskHandlerMap) LookupTask(dagID, taskID string) (bundle.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.handlers[dagID][taskID]
	return task, exists
}

func (m *taskHandlerMap) ListTaskHandlers() []bundle.TaskHandlerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return slices.Clone(m.order)
}

// dagMap holds the registered Dags by dag_id, in registration order.
type dagMap struct {
	mu    sync.Mutex
	dags  map[string]*DagRef
	order []*DagRef
}

func (m *dagMap) add(dag *DagRef) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.dags[dag.dagID]; exists {
		panic(fmt.Sprintf("airflow.BundleRef.Register: Dag %q is already registered", dag.dagID))
	}
	dag.markRegistered()
	if m.dags == nil {
		m.dags = make(map[string]*DagRef)
	}
	m.dags[dag.dagID] = dag
	m.order = append(m.order, dag)
}

// ListDagSourceFiles returns the file that declared each registered Dag, by dag_id.
func (m *dagMap) ListDagSourceFiles() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()

	files := make(map[string]string, len(m.dags))
	for id, dag := range m.dags {
		files[id] = dag.file
	}
	return files
}

// lookupTask returns the task of a registered Dag. It is the Go function of a task from DagRef.Task,
// or a bundle.TriggerTask with the options of a task from TriggerDagRun, which the runtime runs
// itself.
func (m *dagMap) lookupTask(dagID, taskID string) (bundle.Task, bool) {
	m.mu.Lock()
	dag, ok := m.dags[dagID]
	m.mu.Unlock()
	if !ok {
		return nil, false
	}

	dag.mu.Lock()
	defer dag.mu.Unlock()
	ref, ok := dag.tasksByID[taskID]
	if !ok {
		return nil, false
	}
	if ref.triggerDagRun != nil {
		return &bundle.TriggerTask{Spec: triggerSpec(*ref.triggerDagRun)}, true
	}
	if ref.task == nil {
		return nil, false
	}
	return ref.task, true
}

func (m *dagMap) has(dagID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, exists := m.dags[dagID]
	return exists
}

// serialize serializes the Dags in registration order. If a Dag panics, the panic becomes that
// Dag's Err.
func (m *dagMap) serialize(fileloc, relativeFileloc string) []bundle.SerializedDag {
	m.mu.Lock()
	dags := slices.Clone(m.order)
	m.mu.Unlock()

	serialized := make([]bundle.SerializedDag, len(dags))
	for i, dag := range dags {
		serialized[i] = serializeRecovering(dag, fileloc, relativeFileloc)
	}
	return serialized
}

func serializeRecovering(dag *DagRef, fileloc, relativeFileloc string) (s bundle.SerializedDag) {
	s.DagID = dag.dagID
	defer func() {
		if r := recover(); r != nil {
			slog.Error(
				"Dag serialization panicked",
				"dag_id", dag.dagID, "panic", r, "stack", string(debug.Stack()),
			)
			s.Data, s.Err = nil, fmt.Errorf("%v", r)
		}
	}()
	s.Data = dag.serialize(fileloc, relativeFileloc)
	return s
}

// coordinatorSource is what Serve hands to execution.Serve and execution.DumpAirflowMetadata: the
// task handlers and the Dags of one bundle.
type coordinatorSource struct {
	*taskHandlerMap
	dags *dagMap
}

var (
	_ bundle.Bundle          = coordinatorSource{}
	_ bundle.DagSerializer   = coordinatorSource{}
	_ bundle.DagSourceLister = coordinatorSource{}
)

func (s coordinatorSource) SerializeDags(fileloc, relativeFileloc string) []bundle.SerializedDag {
	return s.dags.serialize(fileloc, relativeFileloc)
}

func (s coordinatorSource) ListDagSourceFiles() map[string]string {
	return s.dags.ListDagSourceFiles()
}

// LookupTask finds a task of a Dag from airflow.Dag, then a task handler. A dag_id belongs to only
// one of the two, as Register checks.
func (s coordinatorSource) LookupTask(dagID, taskID string) (bundle.Task, bool) {
	if task, ok := s.dags.lookupTask(dagID, taskID); ok {
		return task, true
	}
	return s.taskHandlerMap.LookupTask(dagID, taskID)
}
