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
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// sourceSchemaRelPath locates the canonical supervisor schema snapshot owned by
// the Python Task SDK, relative to this package directory within the monorepo.
// It is the same file genmodels is generated against (referenced by relative
// path rather than vendored into go-sdk).
const sourceSchemaRelPath = "../../../task-sdk/src/airflow/sdk/execution_time/schema/schema.json"

// TestSupervisorSchemaVersionMatchesSnapshot pins SupervisorSchemaVersion to the
// api_version of the schema the models are generated from, so a schema bump that
// forgets to update the constant (or vice versa) fails loudly. It only runs when
// the task-sdk source is reachable from the checkout, so a standalone go-sdk
// build (without the rest of the monorepo) does not fail.
func TestSupervisorSchemaVersionMatchesSnapshot(t *testing.T) {
	raw, err := os.ReadFile(sourceSchemaRelPath)
	if os.IsNotExist(err) {
		t.Skipf(
			"task-sdk schema source not reachable at %s; skipping version check",
			sourceSchemaRelPath,
		)
	}
	require.NoError(t, err)
	var snapshot struct {
		APIVersion string `json:"api_version"`
	}
	require.NoError(t, json.Unmarshal(raw, &snapshot))
	assert.Equal(t, snapshot.APIVersion, SupervisorSchemaVersion)
}

// marshalBody encodes a wire-shaped body the way the supervisor would, so the
// generated genmodels types can be decoded from realistic msgpack bytes. The
// supervisor encodes datetimes as the msgpack timestamp extension, so tests
// pass time.Time values (not ISO strings) for date-time fields.
func marshalBody(t *testing.T, body any) msgpack.RawMessage {
	t.Helper()
	raw, err := msgpack.Marshal(body)
	require.NoError(t, err)
	return raw
}

// rawToMap decodes a raw msgpack body element back into a generic map so tests
// can assert on individual wire fields of a decoded frame.
func rawToMap(t *testing.T, raw msgpack.RawMessage) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, msgpack.Unmarshal(raw, &m))
	return m
}

func TestDecodeStartupDetails(t *testing.T) {
	logical := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	intervalStart := time.Date(2024, 1, 14, 0, 0, 0, 0, time.UTC)
	intervalEnd := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	startDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	raw := marshalBody(t, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":             "550e8400-e29b-41d4-a716-446655440000",
			"task_id":        "extract",
			"dag_id":         "tutorial_dag",
			"run_id":         "manual__2024-01-15",
			"try_number":     1,
			"dag_version_id": "abc-123",
			"map_index":      -1,
		},
		"dag_rel_path": "dags/tutorial.go",
		"bundle_info": map[string]any{
			"name":    "example_dags",
			"version": "1.0.0",
		},
		"start_date":         startDate,
		"sentry_integration": "",
		// The supervisor nests scheduling timestamps under dag_run, matching
		// task-sdk's TIRunContext / DagRun schema.
		"ti_context": map[string]any{
			"dag_run": map[string]any{
				"dag_id":              "tutorial_dag",
				"run_id":              "manual__2024-01-15",
				"logical_date":        logical,
				"data_interval_start": intervalStart,
				"data_interval_end":   intervalEnd,
			},
			"max_tries":    int64(3),
			"should_retry": true,
		},
	})

	body, err := decodeIncomingBody(raw)
	require.NoError(t, err)
	details, ok := body.(*genmodels.StartupDetails)
	require.True(t, ok, "expected *StartupDetails, got %T", body)

	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", details.TI.ID)
	assert.Equal(t, "extract", details.TI.TaskID)
	assert.Equal(t, "tutorial_dag", details.TI.DagID)
	assert.Equal(t, "manual__2024-01-15", details.TI.RunID)
	assert.Equal(t, 1, details.TI.TryNumber)
	require.NotNil(t, details.TI.MapIndex)
	assert.Equal(t, -1, *details.TI.MapIndex)
	assert.Equal(t, "dags/tutorial.go", details.DagRelPath)
	assert.Equal(t, "example_dags", details.BundleInfo.Name)
	assert.Equal(t, "1.0.0", ifaceString(details.BundleInfo.Version))
	// msgpack decodes the timestamp extension into the local zone; compare the
	// instant in UTC rather than the zoned representation.
	assert.Equal(t, startDate, details.StartDate.UTC())
	assert.Equal(t, logical, ifaceTimePtr(details.TIContext.DagRun.LogicalDate).UTC())
	assert.Equal(t, intervalStart, ifaceTimePtr(details.TIContext.DagRun.DataIntervalStart).UTC())
	assert.Equal(t, intervalEnd, ifaceTimePtr(details.TIContext.DagRun.DataIntervalEnd).UTC())
	// max_tries / should_retry drive the retry decision and live at the top
	// level of ti_context, not under dag_run.
	assert.Equal(t, 3, details.TIContext.MaxTries)
	assert.True(t, details.TIContext.ShouldRetry)
}

func TestDecodeStartupDetails_MissingOptionalTimestamps(t *testing.T) {
	// start_date and ti_context fields are optional on the wire; omitting them
	// must still decode cleanly with zero/nil values rather than erroring.
	raw := marshalBody(t, map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":      "550e8400-e29b-41d4-a716-446655440000",
			"task_id": "t", "dag_id": "d", "run_id": "r",
			"try_number": 1,
		},
	})
	body, err := decodeIncomingBody(raw)
	require.NoError(t, err)
	details := body.(*genmodels.StartupDetails)
	assert.True(t, details.StartDate.IsZero())
	assert.Nil(t, ifaceTimePtr(details.TIContext.DagRun.LogicalDate))
	assert.Nil(t, ifaceTimePtr(details.TIContext.DagRun.DataIntervalStart))
	assert.Nil(t, ifaceTimePtr(details.TIContext.DagRun.DataIntervalEnd))
	// map_index is a pointer now, so an omitted key decodes to nil (the natural
	// "unmapped") rather than being seeded to the -1 sentinel.
	assert.Nil(t, details.TI.MapIndex)
}

func TestDecodeStartupDetails_MissingRequiredTIFieldFails(t *testing.T) {
	completeTI := func() map[string]any {
		return map[string]any{
			"id":         "550e8400-e29b-41d4-a716-446655440000",
			"task_id":    "t",
			"dag_id":     "d",
			"run_id":     "r",
			"try_number": 1,
		}
	}

	tests := []struct {
		missingKey string
		wantErr    string
	}{
		{missingKey: "id", wantErr: "ti.id"},
		{missingKey: "task_id", wantErr: "ti.task_id"},
		{missingKey: "dag_id", wantErr: "ti.dag_id"},
		{missingKey: "run_id", wantErr: "ti.run_id"},
		{missingKey: "try_number", wantErr: "ti.try_number"},
	}
	for _, tc := range tests {
		t.Run("missing "+tc.missingKey, func(t *testing.T) {
			ti := completeTI()
			delete(ti, tc.missingKey)
			raw := marshalBody(t, map[string]any{"type": "StartupDetails", "ti": ti})
			_, err := decodeIncomingBody(raw)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestDecodeConnectionResult(t *testing.T) {
	raw := marshalBody(t, map[string]any{
		"type":      "ConnectionResult",
		"conn_id":   "my_db",
		"conn_type": "postgres",
		"host":      "db.example.com",
		"schema":    "mydb",
		"login":     "user",
		"password":  "secret",
		"port":      5432,
		"extra":     `{"sslmode":"require"}`,
	})

	var result genmodels.ConnectionResult
	require.NoError(t, decodeBody(raw, &result))
	assert.Equal(t, "my_db", result.ConnID)
	assert.Equal(t, "postgres", result.ConnType)
	assert.Equal(t, "db.example.com", ifaceString(result.Host))
	assert.Equal(t, "mydb", ifaceString(result.Schema))
	assert.Equal(t, "user", *ifaceStringPtr(result.Login))
	assert.Equal(t, "secret", *ifaceStringPtr(result.Password))
	assert.Equal(t, 5432, ifaceInt(result.Port, 0))
}

// TestConnectionResultNullableCredentials covers the shapes login / password
// can take on the wire: absent, explicit nil, explicit "", and a real value.
// Empty-string and absent must be distinguishable so URI building in
// sdk.Connection picks the same branch the HTTP-backed SDK would.
func TestConnectionResultNullableCredentials(t *testing.T) {
	empty := ""
	user := "user"
	tests := []struct {
		name         string
		body         map[string]any
		wantLogin    *string
		wantPassword *string
	}{
		{
			name:         "absent keys decode to nil",
			body:         map[string]any{"type": "ConnectionResult", "conn_id": "c"},
			wantLogin:    nil,
			wantPassword: nil,
		},
		{
			name: "explicit nil decodes to nil",
			body: map[string]any{
				"type": "ConnectionResult", "conn_id": "c",
				"login": nil, "password": nil,
			},
			wantLogin:    nil,
			wantPassword: nil,
		},
		{
			name: "empty string is preserved",
			body: map[string]any{
				"type": "ConnectionResult", "conn_id": "c",
				"login": "", "password": "",
			},
			wantLogin:    &empty,
			wantPassword: &empty,
		},
		{
			name: "non-empty string is preserved",
			body: map[string]any{
				"type": "ConnectionResult", "conn_id": "c",
				"login": "user", "password": "secret",
			},
			wantLogin:    &user,
			wantPassword: ptr("secret"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var result genmodels.ConnectionResult
			require.NoError(t, decodeBody(marshalBody(t, tc.body), &result))

			gotLogin := ifaceStringPtr(result.Login)
			gotPassword := ifaceStringPtr(result.Password)
			if tc.wantLogin == nil {
				assert.Nil(t, gotLogin)
			} else {
				require.NotNil(t, gotLogin)
				assert.Equal(t, *tc.wantLogin, *gotLogin)
			}
			if tc.wantPassword == nil {
				assert.Nil(t, gotPassword)
			} else {
				require.NotNil(t, gotPassword)
				assert.Equal(t, *tc.wantPassword, *gotPassword)
			}
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestDecodeVariableResult(t *testing.T) {
	raw := marshalBody(
		t,
		map[string]any{"type": "VariableResult", "key": "my_var", "value": "hello"},
	)
	var result genmodels.VariableResult
	require.NoError(t, decodeBody(raw, &result))
	assert.Equal(t, "my_var", result.Key)
	assert.Equal(t, "hello", result.Value)
}

func TestDecodeXComResult(t *testing.T) {
	raw := marshalBody(t, map[string]any{
		"type":  "XComResult",
		"key":   "return_value",
		"value": map[string]any{"data": "processed"},
	})
	var result genmodels.XComResult
	require.NoError(t, decodeBody(raw, &result))
	assert.Equal(t, "return_value", result.Key)
	valMap, ok := result.Value.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "processed", valMap["data"])
}

// decodeToMap round-trips an outbound message struct back through msgpack into
// a generic map so tests can assert on the exact wire keys/values produced.
func decodeToMap(t *testing.T, msg any) map[string]any {
	t.Helper()
	raw, err := msgpack.Marshal(msg)
	require.NoError(t, err)
	var m map[string]any
	require.NoError(t, msgpack.Unmarshal(raw, &m))
	return m
}

func TestGetConnectionWire(t *testing.T) {
	m := decodeToMap(t, genmodels.GetConnection{Type: genmodels.TypeGetConnection, ConnID: "my_db"})
	assert.Equal(t, "GetConnection", m["type"])
	assert.Equal(t, "my_db", m["conn_id"])
}

func TestGetVariableWire(t *testing.T) {
	m := decodeToMap(t, genmodels.GetVariable{Type: genmodels.TypeGetVariable, Key: "my_var"})
	assert.Equal(t, "GetVariable", m["type"])
	assert.Equal(t, "my_var", m["key"])
}

func TestGetXComWire(t *testing.T) {
	t.Run("with map_index", func(t *testing.T) {
		m := decodeToMap(t, genmodels.GetXCom{
			Type: genmodels.TypeGetXCom, Key: "result",
			DagID: "dag1", TaskID: "task1", RunID: "run1",
			MapIndex: 3, IncludePriorDates: true,
		})
		assert.Equal(t, "GetXCom", m["type"])
		assert.EqualValues(t, 3, m["map_index"])
		assert.Equal(t, true, m["include_prior_dates"])
	})

	t.Run("nil map_index is omitted", func(t *testing.T) {
		m := decodeToMap(t, genmodels.GetXCom{
			Type: genmodels.TypeGetXCom, Key: "result",
			DagID: "d", TaskID: "t", RunID: "r",
		})
		_, hasMapIndex := m["map_index"]
		assert.False(t, hasMapIndex)
	})
}

func TestSetXComWire(t *testing.T) {
	t.Run("nil map_index is omitted", func(t *testing.T) {
		// Unmapped tasks omit map_index entirely, matching Python's
		// SetXCom.map_index = None semantics; a -1 sentinel would conflate
		// "unmapped" with "explicit index -1".
		m := decodeToMap(t, genmodels.SetXCom{
			Type: genmodels.TypeSetXCom, Key: "output", Value: 42,
			DagID: "dag1", TaskID: "task1", RunID: "run1",
		})
		assert.Equal(t, "SetXCom", m["type"])
		assert.EqualValues(t, 42, m["value"])
		_, hasMapIndex := m["map_index"]
		assert.False(t, hasMapIndex)
		_, hasMappedLength := m["mapped_length"]
		assert.False(t, hasMappedLength)
	})

	t.Run("non-nil map_index is emitted", func(t *testing.T) {
		m := decodeToMap(t, genmodels.SetXCom{
			Type: genmodels.TypeSetXCom, Key: "output", Value: 42,
			DagID: "dag1", TaskID: "task1", RunID: "run1", MapIndex: 3,
		})
		assert.EqualValues(t, 3, m["map_index"])
	})
}

func TestSucceedTaskWire(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 123456789, time.UTC)
	m := decodeToMap(t, genmodels.SucceedTask{
		Type:         genmodels.TypeSucceedTask,
		EndDate:      endDate,
		TaskOutlets:  &genmodels.TaskOutlets{},
		OutletEvents: &genmodels.OutletEvents{},
	})
	assert.Equal(t, "SucceedTask", m["type"])
	// end_date round-trips through the msgpack timestamp extension as time.Time
	// (decoded in the local zone), preserving sub-second precision.
	assert.Equal(t, endDate, m["end_date"].(time.Time).UTC())
	// task_outlets / outlet_events must reach the supervisor as empty lists, not
	// be omitted: the supervisor rejects a null/absent value for these fields.
	require.Contains(t, m, "task_outlets")
	assert.Empty(t, m["task_outlets"])
	require.Contains(t, m, "outlet_events")
	assert.Empty(t, m["outlet_events"])
}

func TestTaskStateWire(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	m := decodeToMap(t, genmodels.TaskState{
		Type: genmodels.TypeTaskState, State: genmodels.TaskStateStateFailed, EndDate: endDate,
	})
	assert.Equal(t, "TaskState", m["type"])
	assert.Equal(t, "failed", m["state"])
}

func TestRetryTaskWire(t *testing.T) {
	endDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	m := decodeToMap(t, genmodels.RetryTask{
		Type:        genmodels.TypeRetryTask,
		EndDate:     endDate,
		RetryReason: "boom",
	})
	assert.Equal(t, "RetryTask", m["type"])
	assert.Equal(t, "boom", m["retry_reason"])
}

func TestTaskStateConstants_WireValues(t *testing.T) {
	// Pin each enum constant to the exact wire string Python's
	// TaskInstanceState expects. Renaming these constants is fine; changing the
	// wire value would silently break the protocol.
	cases := map[genmodels.TaskStateState]string{
		genmodels.TaskStateStateFailed:  "failed",
		genmodels.TaskStateStateRemoved: "removed",
		genmodels.TaskStateStateSkipped: "skipped",
	}
	for state, wire := range cases {
		assert.Equal(t, wire, string(state))
		m := decodeToMap(t, genmodels.TaskState{Type: genmodels.TypeTaskState, State: state})
		assert.Equal(t, wire, m["state"], "wire value for %s", state)
	}
}

func TestDecodeIncomingBodyDispatch(t *testing.T) {
	t.Run("StartupDetails", func(t *testing.T) {
		raw := marshalBody(t, map[string]any{
			"type": "StartupDetails",
			"ti": map[string]any{
				"id":      "550e8400-e29b-41d4-a716-446655440000",
				"task_id": "t", "dag_id": "d", "run_id": "r", "try_number": 1,
			},
		})
		result, err := decodeIncomingBody(raw)
		require.NoError(t, err)
		_, ok := result.(*genmodels.StartupDetails)
		assert.True(t, ok)
	})

	t.Run("ErrorResponse", func(t *testing.T) {
		raw := marshalBody(t, map[string]any{"type": "ErrorResponse", "error": "GENERIC_ERROR"})
		result, err := decodeIncomingBody(raw)
		require.NoError(t, err)
		_, ok := result.(*genmodels.ErrorResponse)
		assert.True(t, ok)
	})

	t.Run("nil", func(t *testing.T) {
		result, err := decodeIncomingBody(nil)
		require.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("unknown type", func(t *testing.T) {
		_, err := decodeIncomingBody(marshalBody(t, map[string]any{"type": "UnknownMsg"}))
		assert.Error(t, err)
	})
}

func TestPeekBodyType(t *testing.T) {
	assert.Equal(
		t,
		"GetConnection",
		peekBodyType(marshalBody(t, map[string]any{"type": "GetConnection"})),
	)
	assert.Equal(t, "", peekBodyType(nil))
	assert.Equal(t, "", peekBodyType(msgpack.RawMessage{0xc0})) // msgpack nil
}

func TestApiErrorFromFrame(t *testing.T) {
	t.Run("error element of 3-tuple", func(t *testing.T) {
		f := IncomingFrame{
			Body: marshalBody(t, map[string]any{"type": "ConnectionResult"}),
			Err: marshalBody(
				t,
				map[string]any{"error": "CONNECTION_NOT_FOUND", "detail": map[string]any{"k": "v"}},
			),
		}
		apiErr := apiErrorFromFrame(f)
		require.NotNil(t, apiErr)
		assert.Equal(t, "CONNECTION_NOT_FOUND", apiErr.Err)
		assert.Equal(t, map[string]any{"k": "v"}, apiErr.Detail)
	})

	t.Run("ErrorResponse body of 2-tuple", func(t *testing.T) {
		f := IncomingFrame{
			Body: marshalBody(
				t,
				map[string]any{"type": "ErrorResponse", "error": "VARIABLE_NOT_FOUND"},
			),
		}
		apiErr := apiErrorFromFrame(f)
		require.NotNil(t, apiErr)
		assert.Equal(t, "VARIABLE_NOT_FOUND", apiErr.Err)
	})

	t.Run("nil error element", func(t *testing.T) {
		f := IncomingFrame{
			Body: marshalBody(t, map[string]any{"type": "ConnectionResult"}),
			Err:  msgpack.RawMessage{0xc0},
		}
		assert.Nil(t, apiErrorFromFrame(f))
	})

	t.Run("non-error response", func(t *testing.T) {
		f := IncomingFrame{
			Body: marshalBody(t, map[string]any{"type": "ConnectionResult", "conn_id": "c"}),
		}
		assert.Nil(t, apiErrorFromFrame(f))
	})

	t.Run("off-contract detail still recovers the error code", func(t *testing.T) {
		// detail is a string instead of the schema's object|null; the typed
		// error code must survive so translateApiError maps it correctly.
		f := IncomingFrame{
			Err: marshalBody(
				t,
				map[string]any{"error": "VARIABLE_NOT_FOUND", "detail": "not a map"},
			),
		}
		apiErr := apiErrorFromFrame(f)
		require.NotNil(t, apiErr)
		assert.Equal(t, "VARIABLE_NOT_FOUND", apiErr.Err)
	})
}

func TestIfaceInt(t *testing.T) {
	for _, v := range []any{
		int(42), int8(42), int16(42), int32(42), int64(42),
		uint(42), uint8(42), uint16(42), uint32(42), uint64(42),
		float32(42), float64(42),
	} {
		assert.Equal(t, 42, ifaceInt(v, -1))
	}
	assert.Equal(t, -1, ifaceInt(nil, -1))
	assert.Equal(t, -1, ifaceInt("not a number", -1))
}

func TestIfaceStringHelpers(t *testing.T) {
	assert.Equal(t, "v", ifaceString("v"))
	assert.Equal(t, "", ifaceString(nil))
	assert.Equal(t, "", ifaceString(42))

	assert.Nil(t, ifaceStringPtr(nil))
	assert.Nil(t, ifaceStringPtr(42))
	if p := ifaceStringPtr(""); assert.NotNil(t, p) {
		assert.Equal(t, "", *p) // explicit "" stays distinct from "not set"
	}
}

func TestIfaceTimePtr(t *testing.T) {
	assert.Nil(t, ifaceTimePtr(nil))
	assert.Nil(t, ifaceTimePtr("not a time"))
	now := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if p := ifaceTimePtr(now); assert.NotNil(t, p) {
		assert.Equal(t, now, *p)
	}
}

func TestIsNilRaw(t *testing.T) {
	assert.True(t, isNilRaw(nil))
	assert.True(t, isNilRaw(msgpack.RawMessage{}))
	assert.True(t, isNilRaw(msgpack.RawMessage{0xc0}))
	assert.False(t, isNilRaw(marshalBody(t, map[string]any{"a": 1})))
}
