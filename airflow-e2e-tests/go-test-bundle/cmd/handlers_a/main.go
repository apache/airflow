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

// Command handlers_a is one of the two Go test bundles of the e2e tests.
// It registers the handlers of the Dag ids in E2E_GO_DYNAMIC_DAG_IDS, the first task of
// go_split_artifacts, two tasks of go_task_handler_failures and the task of go_unbound_stub,
// which the Dag processor never binds.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"

	"github.com/apache/airflow/airflow-e2e-tests/go-test-bundle/handlers"
)

func main() {
	bundle := airflow.Bundle()

	for _, dagID := range handlers.DynamicDagIDs() {
		bundle.Register(airflow.TaskHandler(dagID, "greet", handlers.Greet))
	}
	bundle.Register(
		airflow.TaskHandler("go_split_artifacts", "from_a", handlers.ReportArtifact),

		airflow.TaskHandler("go_task_handler_failures", "takes_two_numbers", handlers.TakesTwoNumbers),
		// handlers_b registers it too, which the Dag processor reports as a handler two artifacts claim.
		airflow.TaskHandler("go_task_handler_failures", "claimed_twice", handlers.ReportArtifact),

		// The Dag processor does not route this Dag's queue, so it never binds the task.
		airflow.TaskHandler("go_unbound_stub", "unbound", handlers.ReportArtifact),
	)

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
