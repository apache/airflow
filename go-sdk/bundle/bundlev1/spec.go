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

package bundlev1

type (
	// TaskInfo describes a registered task.
	TaskInfo struct {
		// ID is the task ID.
		ID string
		// TypeName is the unqualified Go function name.
		TypeName string
		// PkgPath is the Go package path.
		PkgPath string
		// Spec configures the task.
		Spec TaskSpec
		// Downstream lists dependent task IDs in registration order.
		Downstream []string
	}

	// DagInfo describes a registered Dag and its tasks.
	DagInfo struct {
		DagID string
		// Spec configures the Dag.
		Spec  DagSpec
		Tasks []TaskInfo
	}
)

// Bool returns a pointer for nullable spec fields.
func Bool(b bool) *bool {
	return &b
}
