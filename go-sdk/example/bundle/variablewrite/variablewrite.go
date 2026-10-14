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

// Package variablewrite holds the write_and_delete_variable task, which
// round-trips Airflow Variables through the supervisor.
package variablewrite

import (
	"fmt"

	"github.com/apache/airflow/go-sdk/airflow"
)

const (
	// WrittenKey holds the run id of the Dag run that wrote it, so a reader can
	// tell this run's write apart from a previous one.
	WrittenKey = "go_e2e_variable"
	// WrittenDescription is stored alongside WrittenKey.
	WrittenDescription = "written by the Go SDK e2e test"
	// ScratchKey is written and then deleted within the same task.
	ScratchKey = "go_e2e_scratch"
)

// WriteAndDeleteVariable stores the current run id under WrittenKey, then
// writes and deletes ScratchKey to exercise the delete path.
func WriteAndDeleteVariable(actx airflow.Context) error {
	client := actx.Client()
	runID := actx.DagRun().RunID
	if err := client.SetVariable(actx, WrittenKey, runID, WrittenDescription); err != nil {
		return fmt.Errorf("setting %s: %w", WrittenKey, err)
	}
	if err := client.SetVariable(actx, ScratchKey, "scratch", ""); err != nil {
		return fmt.Errorf("setting %s: %w", ScratchKey, err)
	}
	if err := client.DeleteVariable(actx, ScratchKey); err != nil {
		return fmt.Errorf("deleting %s: %w", ScratchKey, err)
	}
	return nil
}
