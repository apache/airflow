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
		// The supervisor nests the scheduling timestamps under dag_run, matching
		// task-sdk's TIRunContext / DagRun schema.
		"ti_context": map[string]any{
			"dag_run": map[string]any{
				"dag_id":              "tutorial_dag",
				"run_id":              "manual__2024-01-15",
				"logical_date":        "2024-01-15T00:00:00Z",
				"data_interval_start": "2024-01-14T00:00:00Z",
				"data_interval_end":   "2024-01-15T00:00:00Z",
			},
			"max_tries":    int64(3),
			"should_retry": true,
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
	require.NotNil(t, details.TIContext.LogicalDate)
	assert.Equal(t, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), *details.TIContext.LogicalDate)
	require.NotNil(t, details.TIContext.DataIntervalStart)
	assert.Equal(
		t,
		time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC),
		*details.TIContext.DataIntervalStart,
	)
	require.NotNil(t, details.TIContext.DataIntervalEnd)
	assert.Equal(
		t,
		time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
		*details.TIContext.DataIntervalEnd,
	)
	assert.Equal(t, 3, details.TIContext.MaxTries)
	assert.True(t, details.TIContext.ShouldRetry)
}

func TestDecodeStartupDetails_MalformedStartDate(t *testing.T) {
	// A present-but-malformed start_date must surface as a decode error;
	// silently leaving startDate as the zero time would let tasks run with
	// incorrect provenance.
	m := map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":      "550e8400-e29b-41d4-a716-446655440000",
			"task_id": "t", "dag_id": "d", "run_id": "r",
			"try_number": int64(1),
		},
		"start_date": "not-a-timestamp",
	}
	_, err := decodeStartupDetails(m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "start_date")
}

func TestDecodeStartupDetails_MalformedTIRunContext(t *testing.T) {
	m := map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":      "550e8400-e29b-41d4-a716-446655440000",
			"task_id": "t", "dag_id": "d", "run_id": "r",
			"try_number": int64(1),
		},
		"ti_context": map[string]any{
			"dag_run": map[string]any{
				"logical_date": "garbage",
			},
		},
	}
	_, err := decodeStartupDetails(m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "logical_date")
}

func TestDecodeStartupDetails_RequiresTryNumber(t *testing.T) {
	// try_number is required in Python's TaskInstance model; a missing or
	// wrong-typed value must surface as a decode error rather than silently
	// defaulting and masking supervisor/runtime version-drift bugs.
	t.Run("missing", func(t *testing.T) {
		m := map[string]any{
			"type": "StartupDetails",
			"ti": map[string]any{
				"id":      "550e8400-e29b-41d4-a716-446655440000",
				"task_id": "t", "dag_id": "d", "run_id": "r",
			},
		}
		_, err := decodeStartupDetails(m)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "try_number")
	})

	t.Run("wrong type", func(t *testing.T) {
		m := map[string]any{
			"type": "StartupDetails",
			"ti": map[string]any{
				"id":         "550e8400-e29b-41d4-a716-446655440000",
				"task_id":    "t",
				"dag_id":     "d",
				"run_id":     "r",
				"try_number": "1",
			},
		}
		_, err := decodeStartupDetails(m)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "try_number")
	})
}

func TestDecodeStartupDetails_MissingOptionalTimestamps(t *testing.T) {
	// start_date and ti_context fields are optional; omitting them must
	// still decode cleanly (no error, zero/nil values).
	m := map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":      "550e8400-e29b-41d4-a716-446655440000",
			"task_id": "t", "dag_id": "d", "run_id": "r",
			"try_number": int64(1),
		},
	}
	details, err := decodeStartupDetails(m)
	require.NoError(t, err)
	assert.True(t, details.StartDate.IsZero())
	assert.Nil(t, details.TIContext.LogicalDate)
	assert.Nil(t, details.TIContext.DataIntervalStart)
	assert.Nil(t, details.TIContext.DataIntervalEnd)
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
	require.NotNil(t, result.Login)
	assert.Equal(t, "user", *result.Login)
	require.NotNil(t, result.Password)
	assert.Equal(t, "secret", *result.Password)
	assert.Equal(t, 5432, result.Port)
}

// TestDecodeConnectionResultNullableCredentials covers the four shapes login /
// password can take on the wire: absent, explicit None, explicit "", and a
// real value. Empty-string and absent must be distinguishable so URI building
// in sdk.Connection picks the same branch the HTTP-backed SDK would.
func TestDecodeConnectionResultNullableCredentials(t *testing.T) {
	empty := ""
	user := "user"
	tests := []struct {
		name         string
		body         map[string]any
		wantLogin    *string
		wantPassword *string
	}{
		{
			name: "absent keys decode to nil",
			body: map[string]any{
				"type":    "ConnectionResult",
				"conn_id": "c",
			},
			wantLogin:    nil,
			wantPassword: nil,
		},
		{
			name: "explicit nil decodes to nil",
			body: map[string]any{
				"type":     "ConnectionResult",
				"conn_id":  "c",
				"login":    nil,
				"password": nil,
			},
			wantLogin:    nil,
			wantPassword: nil,
		},
		{
			name: "empty string is preserved",
			body: map[string]any{
				"type":     "ConnectionResult",
				"conn_id":  "c",
				"login":    "",
				"password": "",
			},
			wantLogin:    &empty,
			wantPassword: &empty,
		},
		{
			name: "non-empty string is preserved",
			body: map[string]any{
				"type":     "ConnectionResult",
				"conn_id":  "c",
				"login":    "user",
				"password": "secret",
			},
			wantLogin:    &user,
			wantPassword: nil, // checked below
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := decodeConnectionResult(tc.body)
			require.NoError(t, err)

			if tc.wantLogin == nil {
				assert.Nil(t, result.Login)
			} else {
				require.NotNil(t, result.Login)
				assert.Equal(t, *tc.wantLogin, *result.Login)
			}

			if tc.name == "non-empty string is preserved" {
				require.NotNil(t, result.Password)
				assert.Equal(t, "secret", *result.Password)
				return
			}
			if tc.wantPassword == nil {
				assert.Nil(t, result.Password)
			} else {
				require.NotNil(t, result.Password)
				assert.Equal(t, *tc.wantPassword, *result.Password)
			}
		})
	}
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
	t.Run("nil map_index is omitted", func(t *testing.T) {
		// Unmapped tasks must omit map_index entirely, matching Python's
		// SetXCom.map_index = None semantics; a -1 sentinel would conflate
		// "unmapped" with "explicit index -1".
		msg := SetXComMsg{
			Key: "output", Value: 42,
			DagID: "dag1", TaskID: "task1", RunID: "run1",
		}
		m := msg.toMap()
		assert.Equal(t, "SetXCom", m["type"])
		assert.Equal(t, 42, m["value"])
		_, hasMapIndex := m["map_index"]
		assert.False(t, hasMapIndex)
		_, hasMappedLength := m["mapped_length"]
		assert.False(t, hasMappedLength)
	})

	t.Run("non-nil map_index is emitted", func(t *testing.T) {
		idx := 3
		msg := SetXComMsg{
			Key: "output", Value: 42,
			DagID: "dag1", TaskID: "task1", RunID: "run1", MapIndex: &idx,
		}
		m := msg.toMap()
		assert.Equal(t, 3, m["map_index"])
	})
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

func TestSucceedTaskMsgToMap_PreservesSubsecondPrecision(t *testing.T) {
	// end_date is formatted with RFC3339Nano so sub-second precision
	// round-trips through asTime (which parses RFC3339Nano). Truncating to
	// whole seconds would lose ordering for closely-spaced terminal events.
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	msg := SucceedTaskMsg{EndDate: endDate}
	m := msg.toMap()
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", m["end_date"])
}

func TestTaskStateMsgToMap(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	msg := TaskStateMsg{State: TaskStateFailed, EndDate: endDate}
	m := msg.toMap()
	assert.Equal(t, "TaskState", m["type"])
	assert.Equal(t, "failed", m["state"])
}

func TestTaskStateMsgToMap_PreservesSubsecondPrecision(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	msg := TaskStateMsg{State: TaskStateFailed, EndDate: endDate}
	m := msg.toMap()
	assert.Equal(t, "2024-01-15T10:30:00.123456789Z", m["end_date"])
}

func TestRetryTaskMsgToMap(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	msg := RetryTaskMsg{EndDate: endDate, Reason: "test error"}
	m := msg.toMap()
	assert.Equal(t, "RetryTask", m["type"])
	assert.Equal(t, "test error", m["retry_reason"])
	assert.Equal(t, "2024-01-15T10:30:00Z", m["end_date"])
}

func TestTaskStateConstants_WireValues(t *testing.T) {
	// Pin each enum constant to the exact wire string Python's
	// TaskInstanceState expects. Renaming these constants is fine;
	// changing the wire value would silently break the protocol.
	cases := map[TaskState]string{
		TaskStateFailed:  "failed",
		TaskStateRemoved: "removed",
		TaskStateSkipped: "skipped",
	}
	for state, wire := range cases {
		assert.Equal(t, wire, string(state))
		m := TaskStateMsg{State: state}.toMap()
		assert.Equal(t, wire, m["state"], "wire value for %s", state)
	}
}

func TestDecodeIncomingBodyDispatch(t *testing.T) {
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
