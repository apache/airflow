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

// Package airflowmetadata defines the airflow-metadata manifest wire shape
// shared between the producer (a bundle binary's --airflow-metadata flag,
// emitted from pkg/execution) and the consumer (airflow-go-pack, which decodes
// it and renders the embedded airflow-metadata.yaml). Keeping the definition in
// one place stops the two sides from drifting. The canonical schema is
// airflow-metadata.schema.json in the Task SDK docs.
package airflowmetadata

// FormatVersion is the bundle-spec version emitted manifests conform to.
const FormatVersion = "1.0"

// Manifest is the shape printed by a bundle binary's --airflow-metadata flag
// (YAML by default, JSON under --format json). It mirrors
// airflow-metadata.schema.json minus the source field, which only the packer
// can resolve from the build inputs. The yaml tags let the packer's
// --airflow-metadata flag decode a captured manifest in either JSON or YAML
// (the airflow-metadata.yaml in a bundle).
type Manifest struct {
	AirflowBundleMetadataVersion string         `json:"airflow_bundle_metadata_version" yaml:"airflow_bundle_metadata_version"`
	SDK                          SDK            `json:"sdk"                             yaml:"sdk"`
	Dags                         map[string]Dag `json:"dags"                            yaml:"dags"`
}

// SDK identifies the SDK that produced the bundle.
type SDK struct {
	Language                string `json:"language"                  yaml:"language"`
	Version                 string `json:"version"                   yaml:"version"`
	SupervisorSchemaVersion string `json:"supervisor_schema_version" yaml:"supervisor_schema_version"`
}

// Dag is the static description of a single DAG declared in the bundle.
type Dag struct {
	Tasks []string `json:"tasks" yaml:"tasks"`
}
