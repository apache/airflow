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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeDagFileParseRequest(t *testing.T) {
	m := map[string]any{
		"type":        "DagFileParseRequest",
		"file":        "/path/to/dags.go",
		"bundle_path": "/bundles/my_bundle",
	}

	req, err := decodeDagFileParseRequest(m)
	require.NoError(t, err)
	assert.Equal(t, "/path/to/dags.go", req.File)
	assert.Equal(t, "/bundles/my_bundle", req.BundlePath)
}

func TestDecodeStartupDetails(t *testing.T) {
	m := map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":             "550e8400-e29b-41d4-a716-446655440000",
			"task_id":        "extract",
			"dag_id":         "tutorial_dag",
			"run_id":         "manual__2024-01-15",
			"try_number":     int64(1),
			"dag_version_id": "abc-123",
			"map_index":      int64(-1),
		},
		"dag_rel_path": "dags/tutorial.go",
		"bundle_info": map[string]any{
			"name":    "example_dags",
			"version": "1.0.0",
		},
		"start_date":         "2024-01-15T10:30:00Z",
		"sentry_integration": "",
		"ti_context": map[string]any{
			"logical_date":        "2024-01-15T00:00:00Z",
			"data_interval_start": "2024-01-14T00:00:00Z",
			"data_interval_end":   "2024-01-15T00:00:00Z",
		},
	}

	details, err := decodeStartupDetails(m)
	require.NoError(t, err)

	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", details.TI.ID)
	assert.Equal(t, "extract", details.TI.TaskID)
	assert.Equal(t, "tutorial_dag", details.TI.DagID)
	assert.Equal(t, "manual__2024-01-15", details.TI.RunID)
	assert.Equal(t, 1, details.TI.TryNumber)
	assert.Equal(t, -1, details.TI.MapIndex)
	assert.Equal(t, "dags/tutorial.go", details.DagRelPath)
	assert.Equal(t, "example_dags", details.BundleInfo.Name)
	assert.Equal(t, "1.0.0", details.BundleInfo.Version)
	assert.NotNil(t, details.TIContext.LogicalDate)
}

func TestDecodeConnectionResult(t *testing.T) {
	m := map[string]any{
		"type":      "ConnectionResult",
		"conn_id":   "my_db",
		"conn_type": "postgres",
		"host":      "db.example.com",
		"schema":    "mydb",
		"login":     "user",
		"password":  "secret",
		"port":      int64(5432),
		"extra":     `{"sslmode":"require"}`,
	}

	result, err := decodeConnectionResult(m)
	require.NoError(t, err)
	assert.Equal(t, "my_db", result.ConnID)
	assert.Equal(t, "postgres", result.ConnType)
	assert.Equal(t, "db.example.com", result.Host)
	assert.Equal(t, "mydb", result.Schema)
	assert.Equal(t, "user", result.Login)
	assert.Equal(t, "secret", result.Password)
	assert.Equal(t, 5432, result.Port)
}

func TestDecodeVariableResult(t *testing.T) {
	m := map[string]any{
		"type":  "VariableResult",
		"key":   "my_var",
		"value": "hello",
	}

	result, err := decodeVariableResult(m)
	require.NoError(t, err)
	assert.Equal(t, "my_var", result.Key)
	assert.Equal(t, "hello", result.Value)
}

func TestDecodeXComResult(t *testing.T) {
	m := map[string]any{
		"type":  "XComResult",
		"key":   "return_value",
		"value": map[string]any{"data": "processed"},
	}

	result, err := decodeXComResult(m)
	require.NoError(t, err)
	assert.Equal(t, "return_value", result.Key)
	valMap, ok := result.Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "processed", valMap["data"])
}

func TestDecodeErrorResponseNil(t *testing.T) {
	assert.Nil(t, decodeErrorResponse(nil))
}

func TestGetConnectionMsgToMap(t *testing.T) {
	msg := GetConnectionMsg{ConnID: "my_db"}
	m := msg.toMap()
	assert.Equal(t, "GetConnection", m["type"])
	assert.Equal(t, "my_db", m["conn_id"])
}

func TestGetVariableMsgToMap(t *testing.T) {
	msg := GetVariableMsg{Key: "my_var"}
	m := msg.toMap()
	assert.Equal(t, "GetVariable", m["type"])
	assert.Equal(t, "my_var", m["key"])
}

func TestGetXComMsgToMapWithMapIndex(t *testing.T) {
	mapIdx := 3
	msg := GetXComMsg{
		Key:               "result",
		DagID:             "dag1",
		TaskID:            "task1",
		RunID:             "run1",
		MapIndex:          &mapIdx,
		IncludePriorDates: true,
	}
	m := msg.toMap()
	assert.Equal(t, "GetXCom", m["type"])
	assert.Equal(t, 3, m["map_index"])
	assert.Equal(t, true, m["include_prior_dates"])
}

func TestGetXComMsgToMapNilMapIndex(t *testing.T) {
	msg := GetXComMsg{Key: "result", DagID: "d", TaskID: "t", RunID: "r"}
	m := msg.toMap()
	_, hasMapIndex := m["map_index"]
	assert.False(t, hasMapIndex)
}

func TestSetXComMsgToMap(t *testing.T) {
	msg := SetXComMsg{
		Key: "output", Value: 42,
		DagID: "dag1", TaskID: "task1", RunID: "run1", MapIndex: -1,
	}
	m := msg.toMap()
	assert.Equal(t, "SetXCom", m["type"])
	assert.Equal(t, 42, m["value"])
	_, hasMappedLength := m["mapped_length"]
	assert.False(t, hasMappedLength)
}

func TestSucceedTaskMsgToMap(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	msg := SucceedTaskMsg{EndDate: endDate}
	m := msg.toMap()
	assert.Equal(t, "SucceedTask", m["type"])
	assert.Equal(t, "2024-01-15T10:30:00Z", m["end_date"])
	assert.Equal(t, []any{}, m["task_outlets"])
	assert.Equal(t, []any{}, m["outlet_events"])
}

func TestTaskStateMsgToMap(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	msg := TaskStateMsg{State: "failed", EndDate: endDate}
	m := msg.toMap()
	assert.Equal(t, "TaskState", m["type"])
	assert.Equal(t, "failed", m["state"])
}

func TestDecodeIncomingBodyDispatch(t *testing.T) {
	t.Run("DagFileParseRequest", func(t *testing.T) {
		body := map[string]any{"type": "DagFileParseRequest", "file": "x", "bundle_path": "y"}
		result, err := decodeIncomingBody(body)
		require.NoError(t, err)
		_, ok := result.(*DagFileParseRequest)
		assert.True(t, ok)
	})

	t.Run("ConnectionResult", func(t *testing.T) {
		body := map[string]any{"type": "ConnectionResult", "conn_id": "x"}
		result, err := decodeIncomingBody(body)
		require.NoError(t, err)
		_, ok := result.(*ConnectionResult)
		assert.True(t, ok)
	})

	t.Run("nil", func(t *testing.T) {
		result, err := decodeIncomingBody(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("unknown type", func(t *testing.T) {
		_, err := decodeIncomingBody(map[string]any{"type": "UnknownMsg"})
		assert.Error(t, err)
	})
}

func TestAsTime(t *testing.T) {
	t.Run("from string", func(t *testing.T) {
		ts, err := asTime("2024-01-15T10:30:00Z")
		require.NoError(t, err)
		assert.Equal(t, 2024, ts.Year())
		assert.Equal(t, time.January, ts.Month())
	})

	t.Run("from time.Time", func(t *testing.T) {
		now := time.Now()
		ts, err := asTime(now)
		require.NoError(t, err)
		assert.Equal(t, now, ts)
	})

	t.Run("nil", func(t *testing.T) {
		_, err := asTime(nil)
		assert.Error(t, err)
	})

	t.Run("wrong type", func(t *testing.T) {
		_, err := asTime(42)
		assert.Error(t, err)
	})
}
