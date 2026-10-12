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

package bundle

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/apache/airflow/go-sdk/pkg/binding"
)

// TriggerSpec is what the runtime needs to run a task from airflow.TriggerDagRun. It holds the
// options of the task as the author wrote them, so the runtime applies the defaults. The states
// are the names of Dag run states.
type TriggerSpec struct {
	DagID                 string
	RunID                 string
	Note                  string
	Conf                  map[string]any
	LogicalDate           time.Time
	RunAfter              time.Time
	ResetDagRun           bool
	WaitForCompletion     bool
	SkipWhenAlreadyExists bool
	FailWhenDagIsPaused   bool
	// PokeInterval is nil when the author left it unset.
	PokeInterval *time.Duration
	// AllowedStates is empty when the author left it unset.
	AllowedStates []string
	// FailedStates is nil when the author left it unset, and empty when no state fails the task.
	FailedStates []string
	// Deferrable is nil when the author left it unset.
	Deferrable *bool
}

// TriggerTask is the Task of a task from airflow.TriggerDagRun. It runs no Go code, so the runtime
// recognizes it and runs the task itself.
type TriggerTask struct {
	Spec TriggerSpec
}

var _ Task = (*TriggerTask)(nil)

// Execute always fails, because the runtime runs a TriggerTask without calling it.
func (*TriggerTask) Execute(context.Context, *slog.Logger, []binding.Arg) error {
	return errors.New("a TriggerDagRun task is run by the runtime, so Execute is never called")
}
