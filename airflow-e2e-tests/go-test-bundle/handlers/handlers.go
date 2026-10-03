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

// Package handlers holds the task handlers of the Go test bundles of the e2e tests.
//
// The bundles are separate from the user-facing example in go-sdk, so that fixtures the e2e tests
// use to check failures cannot change what the example registers.
package handlers

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/airflow/go-sdk/airflow"
)

// DynamicDagIDsEnv names the variable that lists the Dag ids handlers_a registers "greet" for.
// The bundle reads it when it starts, so the ids are whatever the environment says then.
const DynamicDagIDsEnv = "E2E_GO_DYNAMIC_DAG_IDS"

// DynamicDagIDs returns the comma-separated Dag ids in [DynamicDagIDsEnv], without empty items.
func DynamicDagIDs() []string {
	var ids []string
	for _, id := range strings.Split(os.Getenv(DynamicDagIDsEnv), ",") {
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

// Greet returns the Dag it runs for and the artifact that runs it, so a test sees which Dag id
// and which file a task ran with.
func Greet(actx airflow.Context) (map[string]any, error) {
	artifact, err := artifactName()
	if err != nil {
		return nil, err
	}
	return map[string]any{"dag_id": actx.TaskInstance().DagID, "artifact": artifact}, nil
}

// ReportArtifact returns the artifact that runs it, so a test sees which file ran a task.
func ReportArtifact(actx airflow.Context) (map[string]any, error) {
	artifact, err := artifactName()
	if err != nil {
		return nil, err
	}
	return map[string]any{"artifact": artifact}, nil
}

// TakesTwoNumbers takes flat parameters, so a Dag file calls it with positional arguments. A call
// with any other number of arguments does not match.
func TakesTwoNumbers(actx airflow.Context, first int, second int) error {
	actx.Logger().InfoContext(actx, "Took two numbers", "first", first, "second", second)
	return nil
}

func artifactName() (string, error) {
	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot tell which artifact runs this task: %w", err)
	}
	return filepath.Base(executable), nil
}
