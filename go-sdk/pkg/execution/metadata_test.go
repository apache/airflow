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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
)

func sampleManifest() airflowmetadata.Manifest {
	return airflowmetadata.Manifest{
		// "1.0" and a task named "123" must survive a YAML round-trip as strings.
		AirflowBundleMetadataVersion: "1.0",
		SDK: airflowmetadata.SDK{
			Language:                "go",
			Version:                 "(devel)",
			SupervisorSchemaVersion: "2026-06-16",
		},
		Dags: map[string]airflowmetadata.Dag{
			"simple_dag": {Tasks: []string{"extract", "transform", "load"}},
			"odd_dag":    {Tasks: []string{"123"}},
		},
	}
}

func TestParseMetadataFormat(t *testing.T) {
	tests := []struct {
		in      string
		want    MetadataFormat
		wantErr bool
	}{
		{in: "", want: MetadataFormatYAML},
		{in: "yaml", want: MetadataFormatYAML},
		{in: "json", want: MetadataFormatJSON},
		{in: "YAML", wantErr: true},
		{in: "yml", wantErr: true},
		{in: "xml", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseMetadataFormat(tc.in)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEncodeManifest_JSON(t *testing.T) {
	meta := sampleManifest()
	data, err := encodeManifest(meta, MetadataFormatJSON)
	require.NoError(t, err)

	assert.Equal(t, byte('{'), data[0], "JSON output must start with an object")
	assert.Equal(t, byte('\n'), data[len(data)-1], "output must end with a newline")
	assert.NotEqual(
		t,
		byte('\n'),
		data[len(data)-2],
		"output must have exactly one trailing newline",
	)
	assert.Contains(t, string(data), "\n    \"sdk\"", "JSON must be indented four spaces")

	var got airflowmetadata.Manifest
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, meta, got, "JSON must round-trip back to the manifest")
}

func TestEncodeManifest_YAML(t *testing.T) {
	meta := sampleManifest()
	// The empty format is the default and must encode as YAML.
	for _, format := range []MetadataFormat{MetadataFormatYAML, ""} {
		data, err := encodeManifest(meta, format)
		require.NoError(t, err)

		assert.NotEqual(t, byte('{'), data[0], "YAML output must not be JSON")
		assert.Equal(t, byte('\n'), data[len(data)-1], "output must end with a newline")
		assert.NotEqual(
			t,
			byte('\n'),
			data[len(data)-2],
			"output must have exactly one trailing newline",
		)
		assert.Contains(t, string(data), "airflow_bundle_metadata_version:")

		var got airflowmetadata.Manifest
		require.NoError(t, yaml.Unmarshal(data, &got))
		assert.Equal(t, meta, got, "YAML must round-trip back to the manifest")
	}
}

func TestEncodeManifest_UnsupportedFormat(t *testing.T) {
	_, err := encodeManifest(sampleManifest(), MetadataFormat("xml"))
	require.Error(t, err)
}
