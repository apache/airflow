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

// Command handlers_b is the second Go test bundle of the e2e tests.
// It registers the second task of the Dag go_split_artifacts, which handlers_a does not register,
// and a handler of go_task_handler_failures that handlers_a registers as well.
package main

import (
	"log"

	"github.com/apache/airflow/go-sdk/airflow"

	"github.com/apache/airflow/airflow-e2e-tests/go-test-bundle/handlers"
)

func main() {
	bundle := airflow.Bundle()

	bundle.Register(
		airflow.TaskHandler("go_split_artifacts", "from_b", handlers.ReportArtifact),
		airflow.TaskHandler("go_task_handler_failures", "claimed_twice", handlers.ReportArtifact),
	)

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}
