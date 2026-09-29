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

// DagSpec holds the attributes of a Dag other than its dag_id. [Dag] takes one.
type DagSpec struct{}

// TaskSpec holds the attributes of a task. [DagRef.Task] takes at most one per task.
type TaskSpec struct {
	// TaskID is the task_id of the task. When TaskID is empty, the task_id is the name of the
	// Go function that the task runs.
	TaskID string
}
