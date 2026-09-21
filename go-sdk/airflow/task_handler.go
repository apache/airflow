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
	"reflect"

	"github.com/apache/airflow/go-sdk/internal/bundle"
)

type taskHandler struct {
	dagId, taskId string
	task          bundle.Task
}

func (*taskHandler) registerable() {}

// TaskHandler makes fn the Go body of a task that a Python Dag declares with @task.stub.
// Pass what it returns to [BundleRef.Register].
//
// dagId is the dag_id of that Python Dag, and taskId is the task_id of the stub task.
//
// fn takes a [Context] first, as the package documentation describes. Every parameter after
// the Context is data, filled from the arguments of the Python stub's TaskFlow call.
// fn returns either error or (result, error).
// A non-nil error fails the task, and a non-nil result is pushed as the task's return-value XCom.
//
// TaskHandler checks the signature of fn and panics if the check fails, for example when fn is
// not a function, does not take a Context first, or does not return an error.
// main calls TaskHandler before Serve, so a handler that fails the check stops the executable as
// soon as it starts instead of when the task first runs.
func TaskHandler(dagId, taskId string, fn any) Registerable {
	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		panic(
			fmt.Sprintf("airflow.TaskHandler(%q, %q): fn is %T, not a function", dagId, taskId, fn),
		)
	}
	if v.IsNil() {
		panic(fmt.Sprintf("airflow.TaskHandler(%q, %q): fn is a nil %T", dagId, taskId, fn))
	}
	task, err := bundle.NewTaskFunction(fn)
	if err != nil {
		panic(fmt.Sprintf("airflow.TaskHandler(%q, %q): %v", dagId, taskId, err))
	}
	return &taskHandler{dagId: dagId, taskId: taskId, task: task}
}
