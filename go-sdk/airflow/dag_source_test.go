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

package airflow

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/apache/airflow/go-sdk/internal/airflowmetadata"
)

func TestDagRecordsCallerFile(t *testing.T) {
	assert.Equal(t, "dag_source_test.go", filepath.Base(Dag("direct").file))
}

func TestDagFromFactoryRecordsFactoryFile(t *testing.T) {
	assert.Equal(t, "dag_factory_test.go", filepath.Base(newFactoryDag("built").file))
}

func TestServeListsDagSourceFiles(t *testing.T) {
	b := Bundle()
	b.Register(
		TaskHandler("py_etl", "extract", noop),
		Dag("direct"),
		newFactoryDag("built"),
	)

	var stdout bytes.Buffer
	require.NoError(t, b.serve([]string{"--airflow-metadata"}, &stdout))

	var got airflowmetadata.Manifest
	require.NoError(t, yaml.Unmarshal(stdout.Bytes(), &got))
	require.Len(t, got.DagSourceFiles, 2)
	assert.Equal(t, "dag_source_test.go", filepath.Base(got.DagSourceFiles["direct"]))
	assert.Equal(t, "dag_factory_test.go", filepath.Base(got.DagSourceFiles["built"]))
	assert.NotContains(t, got.DagSourceFiles, "py_etl")
	assert.NotContains(t, got.Dags, "direct", "native Dags stay out of dags")
}

func TestServeOmitsDagSourceFilesWithoutNativeDags(t *testing.T) {
	var stdout bytes.Buffer
	require.NoError(t, etlBundle().serve([]string{"--airflow-metadata"}, &stdout))

	assert.NotContains(t, stdout.String(), "dag_source_files")
}
