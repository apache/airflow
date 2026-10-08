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
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// serializedDags is a DagSerializer that records the paths it was called with.
type serializedDags struct {
	dags              []bundle.SerializedDag
	fileloc, relative string
}

func (s *serializedDags) SerializeDags(fileloc, relativeFileloc string) []bundle.SerializedDag {
	s.fileloc, s.relative = fileloc, relativeFileloc
	return s.dags
}

func serializedDag(dagID string) bundle.SerializedDag {
	return bundle.SerializedDag{
		DagID: dagID,
		Data:  map[string]any{"__version": 3, "dag": map[string]any{"dag_id": dagID}},
	}
}

var etlParseRequest = &genmodels.DagFileParseRequest{
	File:       "/bundles/go/etl",
	BundlePath: "/bundles/go",
	BundleName: "go",
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// wireJSON returns body as the Dag processor receives it, after the frame encoding of SendRequest.
func wireJSON(t *testing.T, body any) string {
	t.Helper()
	payload, err := encodeRequest(0, body)
	require.NoError(t, err)
	frame, err := decodeFrame(payload)
	require.NoError(t, err)
	dec := msgpack.NewDecoder(bytes.NewReader(frame.Body))
	dec.UseLooseInterfaceDecoding(true)
	var decoded any
	require.NoError(t, dec.Decode(&decoded))
	raw, err := json.Marshal(decoded)
	require.NoError(t, err)
	return string(raw)
}

func TestParseDagsAnswersWithTheSerializedDags(t *testing.T) {
	dags := &serializedDags{dags: []bundle.SerializedDag{
		serializedDag("etl"),
		serializedDag("reports"),
	}}

	result := parseDags(dags, etlParseRequest, discardLogger())

	assert.Equal(t, "/bundles/go/etl", dags.fileloc)
	assert.Equal(t, "etl", dags.relative)
	assert.JSONEq(t, `{
		"type": "DagFileParsingResult",
		"fileloc": "/bundles/go/etl",
		"serialized_dags": [
			{"data": {"__version": 3, "dag": {"dag_id": "etl"}}},
			{"data": {"__version": 3, "dag": {"dag_id": "reports"}}}
		]
	}`, wireJSON(t, result))
}

func TestParseDagsAnswersWithNoDagsWhenTheBundleHasNoSerializer(t *testing.T) {
	result := parseDags(nil, etlParseRequest, discardLogger())

	assert.NotNil(t, result.SerializedDags)
	assert.Empty(t, result.SerializedDags)
	assert.Nil(t, result.ImportErrors)
}

func TestParseDagsSendsAnEmptyListForABundleWithoutDags(t *testing.T) {
	result := parseDags(&serializedDags{}, etlParseRequest, discardLogger())

	assert.JSONEq(t, `{
		"type": "DagFileParsingResult",
		"fileloc": "/bundles/go/etl",
		"serialized_dags": []
	}`, wireJSON(t, result))
}

func TestParseDagsReportsEachDagThatCannotBeSerialized(t *testing.T) {
	dags := &serializedDags{dags: []bundle.SerializedDag{
		serializedDag("etl"),
		{DagID: "reports", Err: errors.New("no schema field")},
		serializedDag("cleanup"),
		{DagID: "audit", Err: errors.New("bad field type")},
	}}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))

	result := parseDags(dags, etlParseRequest, logger)

	assert.JSONEq(t, `{
		"type": "DagFileParsingResult",
		"fileloc": "/bundles/go/etl",
		"serialized_dags": [
			{"data": {"__version": 3, "dag": {"dag_id": "etl"}}},
			{"data": {"__version": 3, "dag": {"dag_id": "cleanup"}}}
		],
		"import_errors": {
			"etl": "Dag \"reports\": no schema field\nDag \"audit\": bad field type"
		}
	}`, wireJSON(t, result))

	var logged []map[string]any
	for line := range bytes.Lines(logs.Bytes()) {
		var record map[string]any
		require.NoError(t, json.Unmarshal(line, &record))
		logged = append(logged, record)
	}
	require.Len(t, logged, 3)
	for i, dagID := range []string{"reports", "audit"} {
		assert.Equal(t, "ERROR", logged[i]["level"])
		assert.Equal(t, dagID, logged[i]["dag_id"])
	}
	assert.Equal(t, "INFO", logged[2]["level"])
	assert.Equal(t, []any{"etl", "reports", "cleanup", "audit"}, logged[2]["dag_ids"])
	assert.EqualValues(t, 2, logged[2]["serialized"])
	assert.EqualValues(t, 2, logged[2]["import_errors"])
}

func TestComputeRelativeFileloc(t *testing.T) {
	tests := []struct {
		name       string
		file       string
		bundlePath string
		want       string
	}{
		{name: "in the bundle", file: "/bundles/go/etl", bundlePath: "/bundles/go", want: "etl"},
		{
			name:       "in a directory of the bundle",
			file:       "/bundles/go/dags/etl",
			bundlePath: "/bundles/go",
			want:       "dags/etl",
		},
		{
			name:       "the bundle itself",
			file:       "/bundles/go/etl",
			bundlePath: "/bundles/go/etl",
			want:       ".",
		},
		{
			name:       "a name that starts with two dots",
			file:       "/bundles/go/..etl",
			bundlePath: "/bundles/go",
			want:       "..etl",
		},
		{
			name:       "outside the bundle",
			file:       "/bundles/gopher/etl",
			bundlePath: "/bundles/go",
			want:       "/bundles/gopher/etl",
		},
		{
			name:       "the directory that holds the bundle",
			file:       "/bundles",
			bundlePath: "/bundles/go",
			want:       "/bundles",
		},
		{name: "a relative file", file: "etl", bundlePath: "/bundles/go", want: "etl"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, computeRelativeFileloc(tt.file, tt.bundlePath))
		})
	}
}
