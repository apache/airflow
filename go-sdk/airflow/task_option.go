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

// TaskOption is an option to [DagRef.Task]. There are two kinds: a [TaskSpec] sets the
// attributes of the task that DagRef.Task adds, and [Inputs] passes the results of other tasks
// to that task.
//
// Its only method is unexported, so a type outside this package cannot declare it.
// A struct that embeds a TaskSpec or a TaskOption still satisfies the interface, and Task
// panics when it is given one.
type TaskOption interface{ applyTask(*taskConfig) }

type taskConfig struct {
	// specs and inputs keep every TaskSpec and every Inputs passed to DagRef.Task, so that Task
	// can reject a second TaskSpec or a second Inputs instead of merging it into the first.
	specs  []TaskSpec
	inputs [][]*TaskRef
}

func (s TaskSpec) applyTask(c *taskConfig) { c.specs = append(c.specs, s) }
