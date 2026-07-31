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
	"log/slog"
	"runtime"
	"time"

	v1 "github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1server"
	"github.com/apache/airflow/go-sdk/sdk"
)

// Must match the dag_id of the Python stub Dag and the Java bundle.
const combinedDagID = "lang_sdk_combined"

// Set by `-ldflags` at build time; the coordinator finds the bundle by dag_id,
// so the name is only cosmetic.
var (
	bundleName    = "lang_sdk_combined_go"
	bundleVersion = "0.0"
)

type combinedBundle struct{}

var _ v1.BundleProvider = (*combinedBundle)(nil)

func (m *combinedBundle) GetBundleVersion() v1.BundleInfo {
	return v1.BundleInfo{Name: bundleName, Version: &bundleVersion}
}

func (m *combinedBundle) RegisterDags(dagbag v1.Registry) error {
	dag := dagbag.AddDag(combinedDagID)
	// Explicit task ids so the Go tasks are namespaced apart from the Java
	// tasks that share this dag_id in the Python stub.
	dag.AddTaskWithName("go_extract", goExtract)
	dag.AddTaskWithName("go_transform", goTransform)
	return nil
}

func main() {
	if err := bundlev1server.Serve(&combinedBundle{}); err != nil {
		log.Fatal(err)
	}
}

// goExtract returns a map pushed as the task's XCom, mirroring the reference
// example's extract task so the Python downstream can read it.
func goExtract(ctx sdk.TIRunContext, log *slog.Logger) (any, error) {
	log.InfoContext(ctx, "go_extract running")
	return map[string]any{
		"go_version": runtime.Version(),
		"timestamp":  time.Now().UnixNano(),
	}, nil
}

// goTransform reads the my_variable Airflow variable through the coordinator,
// exercising a GetVariable round-trip over the Execution API.
func goTransform(ctx sdk.TIRunContext, client sdk.VariableClient, log *slog.Logger) error {
	val, err := client.GetVariable(ctx, "my_variable")
	if err != nil {
		return err
	}
	log.InfoContext(ctx, "go_transform obtained variable", "my_variable", val)
	return nil
}
