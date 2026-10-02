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

// Package airflowmetadata defines the airflow-metadata manifest wire shape that airflow-go-pack
// renders into the airflow-metadata.yaml it embeds in a bundle. The packer is the only producer;
// the canonical schema is airflow-metadata.schema.json in the Task SDK docs.
package airflowmetadata

// FormatVersion is the bundle-spec version emitted manifests conform to.
const FormatVersion = "1.0"

// Manifest is the part of the manifest the packer builds from the bundle binary and its own SDK.
// It mirrors airflow-metadata.schema.json minus the source and digests fields, which only the
// packer can resolve from the build inputs.
type Manifest struct {
	AirflowBundleMetadataVersion string `json:"airflow_bundle_metadata_version" yaml:"airflow_bundle_metadata_version"`
	SDK                          SDK    `json:"sdk"                             yaml:"sdk"`
}

// SDK identifies the SDK that produced the bundle.
type SDK struct {
	Language                string `json:"language"                  yaml:"language"`
	Version                 string `json:"version"                   yaml:"version"`
	SupervisorSchemaVersion string `json:"supervisor_schema_version" yaml:"supervisor_schema_version"`
}
