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

import "errors"

// TaskOption is an option to [DagRef.Task], [DagRef.If], [DagRef.Switch] and the methods of the
// same names on [TaskGroupRef]. There are two kinds: a [TaskSpec] sets the attributes of the task
// that the method adds, and [Inputs] passes the results of other tasks to that task.
//
// Its only method is unexported, so a type outside this package cannot declare it.
// A struct that embeds a TaskSpec or a TaskOption still satisfies the interface, but the methods
// that take a TaskOption panic when they get such a struct.
type TaskOption interface{ applyTask(*taskConfig) error }

type taskConfig struct {
	spec   TaskSpec
	inputs []Input
	// hasSpec is true once addTask has applied a TaskSpec, and hasInputs is true once it has
	// applied an Inputs. The spec and inputs fields cannot show that an option was applied,
	// because TaskSpec{} leaves spec at its zero value and Inputs() leaves inputs nil.
	hasSpec   bool
	hasInputs bool
}

func (s TaskSpec) applyTask(c *taskConfig) error {
	if c.hasSpec {
		return errors.New(
			"got more than one airflow.TaskSpec; set all of the task's attributes in one TaskSpec",
		)
	}
	c.spec, c.hasSpec = s, true
	return nil
}
