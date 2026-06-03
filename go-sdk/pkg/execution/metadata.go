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

package execution

import (
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
)

// sdkModulePath is the import path of the SDK module. Used to identify the
// SDK version from the bundle binary's build info dependencies.
const sdkModulePath = "github.com/apache/airflow/go-sdk"

// DumpAirflowMetadata runs the bundle's RegisterDags against an in-memory
// recorder and writes the airflow-metadata manifest JSON to stdout. It must
// not start the gRPC server or contact any external services; the recorder is
// the only side effect. airflow-go-pack execs the bundle binary with
// --airflow-metadata and captures this output to build the embedded manifest.
func DumpAirflowMetadata(bundle bundlev1.BundleProvider) error {
	reg := bundlev1.New()
	if err := bundle.RegisterDags(reg); err != nil {
		return fmt.Errorf("registering dags: %w", err)
	}

	enum, ok := reg.(bundlev1.EnumerableBundle)
	if !ok {
		return fmt.Errorf("registry does not implement EnumerableBundle")
	}

	meta := airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: airflowmetadata.FormatVersion,
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 sdkVersion(),
			SupervisorSchemaVersion: SupervisorSchemaVersion,
		},
		Dags: make(map[string]airflowmetadata.Dag),
	}
	for _, dag := range enum.OrderedDags() {
		taskIDs := make([]string, 0, len(dag.Tasks))
		for _, t := range dag.Tasks {
			taskIDs = append(taskIDs, t.ID)
		}
		meta.Dags[dag.DagID] = airflowmetadata.Dag{Tasks: taskIDs}
	}

	data, err := json.MarshalIndent(meta, "", "    ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// sdkVersion returns the version of the SDK module linked into this binary,
// derived from runtime/debug.ReadBuildInfo. Falls back to "(devel)" when
// build info is unavailable (e.g. tests, bundle binaries built from a local
// replace directive).
func sdkVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	if info.Main.Path == sdkModulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	for _, dep := range info.Deps {
		if dep.Path == sdkModulePath {
			if dep.Replace != nil && dep.Replace.Version != "" {
				return dep.Replace.Version
			}
			if dep.Version != "" {
				return dep.Version
			}
		}
	}
	return "(devel)"
}
