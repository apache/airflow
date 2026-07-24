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

import (
	"fmt"
	"time"
)

// This file holds the hand-written companions of the generated spec.gen.go:
// DagSpec (whose DagId/Schedule fields and always-emit/config-fallback keys
// the schema cannot express mechanically) and the registration Info structs
// that carry Go-side identity the schema knows nothing about.

type (
	// DagSpec describes a dag at registration time. DagId is required; every
	// other field is optional, with a zero value meaning "unset" so the
	// scheduler falls back to its serialization-schema default. The field
	// names mirror the keys defined under "dag" in
	// airflow-core/src/airflow/serialization/schema.json.
	DagSpec struct {
		// DagId is the dag id. Required: AddDag panics when it is empty.
		DagId string
		// Schedule is "@once", "@continuous", a cron expression, or "" for
		// NullTimetable (no schedule).
		Schedule                    string
		Description                 string
		StartDate                   time.Time
		EndDate                     time.Time
		Tags                        []string
		DagDisplayName              string
		DocMD                       string
		MaxActiveTasks              int
		MaxActiveRuns               int
		MaxConsecutiveFailedDagRuns int
		DagrunTimeout               time.Duration
		Catchup                     bool
		FailFast                    bool
		RenderTemplateAsNativeObj   bool
		DisableBundleVersioning     bool
		// IsPausedUponCreation has no schema default. nil means "unset"; pass
		// Bool(true) or Bool(false) to set it explicitly.
		IsPausedUponCreation *bool
	}

	// TaskInfo describes a registered task. Coordinator-mode DAG parsing uses
	// it to render the per-task block of a DagFileParsingResult.
	TaskInfo struct {
		// ID is the user-visible task id (TaskSpec.TaskId when set, otherwise
		// the task function's name).
		ID string
		// TypeName is the unqualified Go function name (e.g. "extract").
		TypeName string
		// PkgPath is the Go package path (e.g. "main", "github.com/x/y").
		PkgPath string
		// Spec carries the optional per-task configuration supplied at
		// registration. The zero value means "no overrides".
		Spec TaskSpec
		// Inputs lists the upstream task ids whose return values feed this
		// task's data parameters, in parameter order (the Inputs option).
		Inputs []string
		// Downstream lists task ids that depend on this task, populated as
		// later registrations reference this task via Inputs or After.
		// Order is registration order; the serializer sorts before emit.
		Downstream []string
	}

	// DagInfo describes a registered dag together with its tasks in
	// registration order.
	DagInfo struct {
		DagID string
		// Spec carries the per-dag configuration supplied at registration.
		Spec  DagSpec
		Tasks []TaskInfo
	}
)

// applyTask lets a TaskSpec value be passed directly as a TaskOption to
// Dag.Task. Passing more than one spec panics at registration time.
func (s TaskSpec) applyTask(c *taskConfig) {
	if c.specSet {
		panic(fmt.Errorf("Task accepts at most one TaskSpec"))
	}
	c.spec = s
	c.specSet = true
}
