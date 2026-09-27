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
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// BundleRef holds the task handlers that this executable runs for Airflow.
// [Bundle] returns an empty one.
type BundleRef struct {
	// closed ends registration for everything the bundle can hold, so a kind added later
	// is covered without a flag of its own. Serve sets it; Register reads it.
	closed       atomic.Bool
	taskHandlers taskHandlerMap
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

// Registerable is what [BundleRef.Register] accepts. [TaskHandler] returns one.
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
// Register panics if a task handler with the same dag_id and task_id is already registered,
// and if [BundleRef.Serve] has already been called: registration closes when serving starts.
func (b *BundleRef) Register(items ...Registerable) {
	if b.closed.Load() {
		panic(
			"airflow.BundleRef.Register: Serve has already been called; register everything before Serve",
		)
	}
	for _, item := range items {
		switch item := item.(type) {
		case *taskHandler:
			b.taskHandlers.add(item.dagId, item.taskId, item.task)
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

func (m *taskHandlerMap) add(dagId, taskId string, task bundle.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.handlers == nil {
		m.handlers = make(map[string]map[string]bundle.Task)
	}
	dagHandlers, exists := m.handlers[dagId]
	if !exists {
		dagHandlers = make(map[string]bundle.Task)
		m.handlers[dagId] = dagHandlers
	}
	if _, exists := dagHandlers[taskId]; exists {
		panic(fmt.Sprintf(
			"airflow.BundleRef.Register: task %q of Dag %q is already registered", taskId, dagId,
		))
	}
	dagHandlers[taskId] = task
	m.order = append(m.order, bundle.TaskHandlerInfo{DagID: dagId, TaskID: taskId})
}

func (m *taskHandlerMap) LookupTask(dagId, taskId string) (bundle.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.handlers[dagId][taskId]
	return task, exists
}

func (m *taskHandlerMap) ListTaskHandlers() []bundle.TaskHandlerInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return slices.Clone(m.order)
}
