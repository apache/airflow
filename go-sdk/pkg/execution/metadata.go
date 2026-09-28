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
	"io"
	"runtime/debug"

	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
	"github.com/apache/airflow/go-sdk/internal/bundle"
)

// sdkModulePath is the import path of the SDK module. Used to identify the
// SDK version from the bundle binary's build info dependencies.
const sdkModulePath = "github.com/apache/airflow/go-sdk"

// MetadataFormat selects the encoding DumpAirflowMetadata writes to stdout for
// the bundle binary's --airflow-metadata flag.
type MetadataFormat string

const (
	// MetadataFormatYAML is the default; it matches the airflow-metadata.yaml a
	// bundle embeds, so `mybundle --airflow-metadata > airflow-metadata.yaml`
	// yields a ready-to-use file.
	MetadataFormatYAML MetadataFormat = "yaml"
	// MetadataFormatJSON is opt-in via --format json.
	MetadataFormatJSON MetadataFormat = "json"
)

// ParseMetadataFormat validates a --format value and returns the matching
// MetadataFormat. An empty value defaults to YAML.
func ParseMetadataFormat(s string) (MetadataFormat, error) {
	switch MetadataFormat(s) {
	case "", MetadataFormatYAML:
		return MetadataFormatYAML, nil
	case MetadataFormatJSON:
		return MetadataFormatJSON, nil
	default:
		return "", fmt.Errorf(
			"unsupported --airflow-metadata format %q: want %q or %q",
			s, MetadataFormatYAML, MetadataFormatJSON,
		)
	}
}

// DumpAirflowMetadata writes the bundle's airflow-metadata manifest to w, as YAML
// by default or as JSON when format is MetadataFormatJSON. It only reads the
// registered Dag and task ids, without running a task or calling an external service.
// airflow-go-pack execs the binary with --airflow-metadata and decodes this
// output to build the embedded manifest.
func DumpAirflowMetadata(
	w io.Writer,
	b bundle.EnumerableBundle,
	format MetadataFormat,
) error {
	data, err := encodeManifest(collectManifest(b), format)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func collectManifest(b bundle.EnumerableBundle) airflowmetadata.Manifest {
	meta := airflowmetadata.Manifest{
		AirflowBundleMetadataVersion: airflowmetadata.FormatVersion,
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 sdkVersion(),
			SupervisorSchemaVersion: SupervisorSchemaVersion,
		},
		Dags: make(map[string]airflowmetadata.Dag),
	}
	for _, handler := range b.ListTaskHandlers() {
		dag := meta.Dags[handler.DagID]
		dag.Tasks = append(dag.Tasks, handler.TaskID)
		meta.Dags[handler.DagID] = dag
	}
	return meta
}

// encodeManifest renders the manifest, ensuring exactly one trailing newline
// (yaml.Marshal adds one; JSON does not).
func encodeManifest(meta airflowmetadata.Manifest, format MetadataFormat) ([]byte, error) {
	switch format {
	case MetadataFormatYAML, "":
		return yaml.Marshal(meta)
	case MetadataFormatJSON:
		data, err := json.MarshalIndent(meta, "", "    ")
		if err != nil {
			return nil, err
		}
		return append(data, '\n'), nil
	default:
		return nil, fmt.Errorf("unsupported airflow-metadata format %q", format)
	}
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
