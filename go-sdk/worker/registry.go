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

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"strings"
	"sync"
)

type (
	// task is an interface of an task implementation.
	Task interface {
		Execute(ctx context.Context, logger *slog.Logger) error
	}
	registry struct {
		sync.RWMutex
		taskFuncMap map[string]map[string]Task
	}
)

func newRegistry() *registry {
	return &registry{taskFuncMap: make(map[string]map[string]Task)}
}

func getFnName(fn reflect.Value) string {
	fullName := runtime.FuncForPC(fn.Pointer()).Name()
	parts := strings.Split(fullName, ".")
	fnName := parts[len(parts)-1]
	// Go adds `-fm` suffix to a method names
	return strings.TrimSuffix(fnName, "-fm")
}

func (r *registry) RegisterTask(dagId string, fn any) {
	val := reflect.ValueOf(fn)

	if val.Kind() != reflect.Func {
		panic(fmt.Errorf("task fn was a %s, not a func", val.Kind()))
	}

	fnName := getFnName(val)

	r.RegisterTaskWithName(dagId, fnName, fn)
}

func (r *registry) RegisterTaskWithName(dagId, taskId string, fn any) {
	task, err := NewTaskFunction(fn)
	if err != nil {
		panic(fmt.Errorf("error registering task %q for DAG %q: %w", taskId, dagId, err))
	}

	r.RWMutex.Lock()
	defer r.RWMutex.Unlock()

	dagTasks, exists := r.taskFuncMap[dagId]

	if !exists {
		dagTasks = make(map[string]Task)
		r.taskFuncMap[dagId] = dagTasks
	}

	_, exists = dagTasks[taskId]
	if exists {
		panic(fmt.Errorf("taskId %q is already registered for DAG %q", taskId, dagId))
	}
	dagTasks[taskId] = task
}

func (r *registry) LookupTask(dagId, taskId string) (task Task, exists bool) {
	r.RLock()
	defer r.RUnlock()

	dagTasks, exists := r.taskFuncMap[dagId]
	if !exists {
		return nil, false
	}
	task, exists = dagTasks[taskId]
	return task, exists
}
