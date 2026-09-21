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
	"sync"

	"github.com/apache/airflow/go-sdk/internal/bundlev1"
)

// BundleRef holds the task handlers that this executable runs for Airflow.
// [Bundle] returns an empty one.
type BundleRef struct {
	tasks taskMap
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

// Registraterable is what [BundleRef.Register] accepts. [TaskHandler] returns one.
//
// Its only method is unexported, so a type outside this package cannot declare it.
// A struct that embeds a Registraterable still satisfies the interface, and Register panics
// when it is given one.
type Registraterable interface{ registraterable() }

// Register adds items to the bundle.
//
// A package that defines task handlers can return them as a []Registraterable,
// and main passes that slice as bundle.Register(pkg.Handlers()...).
//
// Register panics if a task handler with the same dag_id and task_id is already registered.
func (b *BundleRef) Register(items ...Registraterable) {
	for _, item := range items {
		switch item := item.(type) {
		case *taskHandler:
			b.tasks.add(item.dagId, item.taskId, item.task)
		default:
			// Either a nil item, or a struct from another package that embeds a Registraterable.
			panic(fmt.Sprintf("airflow.BundleRef.Register: cannot register %T", item))
		}
	}
}

// taskMap holds the registered tasks by dag_id and task_id.
// It also keeps registration order. The --airflow-metadata manifest lists the tasks of each Dag
// in that order.
type taskMap struct {
	mu        sync.RWMutex
	tasks     map[string]map[string]bundlev1.Task
	dagOrder  []string
	taskOrder map[string][]string
}

var (
	_ bundlev1.Bundle           = (*taskMap)(nil)
	_ bundlev1.EnumerableBundle = (*taskMap)(nil)
)

func (m *taskMap) add(dagId, taskId string, task bundlev1.Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tasks == nil {
		m.tasks = make(map[string]map[string]bundlev1.Task)
		m.taskOrder = make(map[string][]string)
	}
	dagTasks, exists := m.tasks[dagId]
	if !exists {
		dagTasks = make(map[string]bundlev1.Task)
		m.tasks[dagId] = dagTasks
		m.dagOrder = append(m.dagOrder, dagId)
	}
	if _, exists := dagTasks[taskId]; exists {
		panic(fmt.Sprintf(
			"airflow.BundleRef.Register: task %q of Dag %q is already registered", taskId, dagId,
		))
	}
	dagTasks[taskId] = task
	m.taskOrder[dagId] = append(m.taskOrder[dagId], taskId)
}

func (m *taskMap) LookupTask(dagId, taskId string) (bundlev1.Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.tasks[dagId][taskId]
	return task, exists
}

func (m *taskMap) OrderedDags() []bundlev1.DagInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]bundlev1.DagInfo, 0, len(m.dagOrder))
	for _, dagId := range m.dagOrder {
		taskIds := m.taskOrder[dagId]
		tasks := make([]bundlev1.TaskInfo, 0, len(taskIds))
		for _, taskId := range taskIds {
			tasks = append(tasks, bundlev1.TaskInfo{ID: taskId})
		}
		out = append(out, bundlev1.DagInfo{DagID: dagId, Tasks: tasks})
	}
	return out
}
