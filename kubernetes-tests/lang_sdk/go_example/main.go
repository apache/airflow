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

// Command go_example is the Go half of the KubernetesExecutor lang-SDK system
// test bundle. It registers the Go tasks of the shared lang_sdk_combined Dag
// (the Java half lives in ../java_example, the Python stub Dag in ../dags). The
// coordinator locates this binary by dag_id, so only the Go tasks are registered
// here; the Java tasks of the same dag_id live in the Java jar.
package main

import (
	"log"
	"runtime"
	"time"

	"github.com/apache/airflow/go-sdk/airflow"
)

// Must match the dag_id of the Python stub Dag and the Java bundle.
const combinedDagID = "lang_sdk_combined"

func main() {
	bundle := airflow.Bundle()

	// The go_ prefix keeps the Go tasks apart from the Java tasks that share
	// this dag_id in the Python stub.
	bundle.Register(
		airflow.TaskHandler(combinedDagID, "go_extract", goExtract),
		airflow.TaskHandler(combinedDagID, "go_transform", goTransform),
	)

	if err := bundle.Serve(); err != nil {
		log.Fatal(err)
	}
}

// goExtract returns a map pushed as the task's XCom, mirroring the reference
// example's extract task so the Python downstream can read it.
func goExtract(actx airflow.Context) (any, error) {
	actx.Logger().InfoContext(actx, "go_extract running")
	return map[string]any{
		"go_version": runtime.Version(),
		"timestamp":  time.Now().UnixNano(),
	}, nil
}

// goTransform reads the my_variable Airflow variable through the coordinator,
// exercising a GetVariable round-trip over the Execution API.
func goTransform(actx airflow.Context) error {
	val, err := actx.Client().GetVariable(actx, "my_variable")
	if err != nil {
		return err
	}
	actx.Logger().InfoContext(actx, "go_transform obtained variable", "my_variable", val)
	return nil
}
