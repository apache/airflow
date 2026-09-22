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
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/sdk"
)

const testTIID = "0199e0e5-1b2c-7c3d-8e4f-5a6b7c8d9e0f"

// TestCoordinatorClientGetVariableEnvOverride verifies that an
// AIRFLOW_VAR_<UPPER(key)> environment override short-circuits the comm
// socket.
func TestCoordinatorClientGetVariableEnvOverride(t *testing.T) {
	t.Setenv("AIRFLOW_VAR_MY_KEY", "env_value")

	// A nil reader would panic if the dispatcher ever read; an empty
	// io.Discard write target would silently accept any request. Use both
	// to assert *no* IO occurred by failing if anything is read or written.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(assertNoReadReader{t: t}, assertNoWriteWriter{t: t}, logger)
	client := NewCoordinatorClient(comm, testTIID)

	val, err := client.GetVariable(context.Background(), "my_key")
	require.NoError(t, err)
	assert.Equal(t, "env_value", val)
}

// TestCoordinatorClientGetVariableNoEnvOverride verifies the supervisor
// round trip still runs when no env override is set.
func TestCoordinatorClientGetVariableNoEnvOverride(t *testing.T) {
	responsePayload, err := encodeRequest(0, map[string]any{
		"type":  "VariableResult",
		"key":   "my_key",
		"value": "supervisor_value",
	})
	require.NoError(t, err)

	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
	client := NewCoordinatorClient(comm, testTIID)

	val, err := client.GetVariable(context.Background(), "my_key")
	require.NoError(t, err)
	assert.Equal(t, "supervisor_value", val)
}

// TestCoordinatorClientErrorTranslation verifies that GetVariable,
// GetConnection, and GetXCom translate the supervisor's *_NOT_FOUND error
// codes into the SDK sentinel errors used by task code.
func TestCoordinatorClientErrorTranslation(t *testing.T) {
	tests := []struct {
		name      string
		errorCode string
		sentinel  error
		call      func(client *CoordinatorClient) error
	}{
		{
			name:      "GetVariable maps VARIABLE_NOT_FOUND",
			errorCode: "VARIABLE_NOT_FOUND",
			sentinel:  sdk.VariableNotFound,
			call: func(client *CoordinatorClient) error {
				_, err := client.GetVariable(context.Background(), "missing")
				return err
			},
		},
		{
			name:      "GetConnection maps CONNECTION_NOT_FOUND",
			errorCode: "CONNECTION_NOT_FOUND",
			sentinel:  sdk.ConnectionNotFound,
			call: func(client *CoordinatorClient) error {
				_, err := client.GetConnection(context.Background(), "missing")
				return err
			},
		},
		{
			name:      "GetXCom maps XCOM_NOT_FOUND",
			errorCode: "XCOM_NOT_FOUND",
			sentinel:  sdk.XComNotFound,
			call: func(client *CoordinatorClient) error {
				_, err := client.GetXCom(
					context.Background(), "dag", "run", "task", nil, "missing", nil,
				)
				return err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
				"type":   "ErrorResponse",
				"error":  tc.errorCode,
				"detail": map[string]any{"msg": "supervisor said no"},
			})
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
			client := NewCoordinatorClient(comm, testTIID)

			err := tc.call(client)
			require.Error(t, err)
			require.True(
				t,
				errors.Is(err, tc.sentinel),
				"expected errors.Is(%v, %v) to be true", err, tc.sentinel,
			)
		})
	}
}

// TestCoordinatorClientErrorPassThrough verifies that unrelated *ApiError
// values (e.g. a generic API_SERVER_ERROR) are returned unchanged.
func TestCoordinatorClientErrorPassThrough(t *testing.T) {
	responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
		"type":   "ErrorResponse",
		"error":  "API_SERVER_ERROR",
		"detail": map[string]any{"msg": "boom"},
	})
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
	client := NewCoordinatorClient(comm, testTIID)

	_, err := client.GetVariable(context.Background(), "any_key")
	require.Error(t, err)
	assert.False(t, errors.Is(err, sdk.VariableNotFound),
		"generic supervisor errors must not be translated to VariableNotFound")
	var apiErr *ApiError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, "API_SERVER_ERROR", apiErr.Err)
}

// TestCoordinatorClientSetVariable verifies the PutVariable frame always
// carries description, sending null when none is given: the supervisor
// validates PutVariable with a required description field.
func TestCoordinatorClientSetVariable(t *testing.T) {
	tests := []struct {
		name            string
		description     string
		wantDescription any
	}{
		{
			name:            "description is sent",
			description:     "row threshold",
			wantDescription: "row threshold",
		},
		{name: "empty description is sent as null", description: "", wantDescription: nil},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, nil, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			require.NoError(
				t,
				client.SetVariable(context.Background(), "my_key", "42", tc.description),
			)

			sent, err := readFrame(&requestBuf)
			require.NoError(t, err)
			assert.Equal(t, map[string]any{
				"type":        "PutVariable",
				"key":         "my_key",
				"value":       "42",
				"description": tc.wantDescription,
			}, rawToMap(t, sent.Body))
		})
	}
}

// TestCoordinatorClientDeleteVariable verifies the DeleteVariable frame sent
// to the supervisor.
func TestCoordinatorClientDeleteVariable(t *testing.T) {
	responsePayload := encodeResponseFrame(
		t,
		0,
		map[string]any{"type": "OKResponse", "ok": true},
		nil,
	)
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
	client := NewCoordinatorClient(comm, testTIID)

	require.NoError(t, client.DeleteVariable(context.Background(), "my_key"))

	sent, err := readFrame(&requestBuf)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"type": "DeleteVariable",
		"key":  "my_key",
	}, rawToMap(t, sent.Body))
}

// TestCoordinatorClientVariableWriteErrors verifies SetVariable and
// DeleteVariable surface a supervisor ErrorResponse to the task.
func TestCoordinatorClientVariableWriteErrors(t *testing.T) {
	tests := []struct {
		name string
		call func(client *CoordinatorClient) error
	}{
		{
			name: "SetVariable",
			call: func(client *CoordinatorClient) error {
				return client.SetVariable(context.Background(), "my_key", "v", "")
			},
		},
		{
			name: "DeleteVariable",
			call: func(client *CoordinatorClient) error {
				return client.DeleteVariable(context.Background(), "my_key")
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
				"type":   "ErrorResponse",
				"error":  "API_SERVER_ERROR",
				"detail": map[string]any{"status_code": 403},
			})
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
			client := NewCoordinatorClient(comm, testTIID)

			var apiErr *ApiError
			require.ErrorAs(t, tc.call(client), &apiErr)
			assert.Equal(t, "API_SERVER_ERROR", apiErr.Err)
		})
	}
}

// TestCoordinatorClientGetConnectionPreservesEmptyCredentials verifies the
// coordinator client forwards an explicitly empty login/password as a
// pointer-to-"" on sdk.Connection rather than nil. Connections that use
// empty credentials intentionally (e.g. a default-blank password) would
// otherwise fall back to "no login set" URI behaviour.
func TestCoordinatorClientGetConnectionPreservesEmptyCredentials(t *testing.T) {
	responsePayload := encodeResponseFrame(t, 0, map[string]any{
		"type":     "ConnectionResult",
		"conn_id":  "c",
		"login":    "",
		"password": "",
	}, nil)
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
	client := NewCoordinatorClient(comm, testTIID)

	conn, err := client.GetConnection(context.Background(), "c")
	require.NoError(t, err)
	require.NotNil(t, conn.Login, "explicit empty-string login must round-trip as &\"\"")
	assert.Equal(t, "", *conn.Login)
	require.NotNil(t, conn.Password, "explicit empty-string password must round-trip as &\"\"")
	assert.Equal(t, "", *conn.Password)
}

// TestCoordinatorClientGetConnectionAbsentCredentials verifies the
// coordinator client leaves sdk.Connection's Login/Password as nil when the
// supervisor omits or sends null for those fields.
func TestCoordinatorClientGetConnectionAbsentCredentials(t *testing.T) {
	responsePayload := encodeResponseFrame(t, 0, map[string]any{
		"type":    "ConnectionResult",
		"conn_id": "c",
	}, nil)
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
	client := NewCoordinatorClient(comm, testTIID)

	conn, err := client.GetConnection(context.Background(), "c")
	require.NoError(t, err)
	assert.Nil(t, conn.Login, "absent login must remain nil")
	assert.Nil(t, conn.Password, "absent password must remain nil")
}

// TestCoordinatorClientPushXComMapIndex verifies the SetXCom frame omits
// map_index for unmapped task instances and propagates a real index when the
// task is dynamically mapped. Sending the -1 sentinel on the wire would be
// silently treated as a separate map element by the supervisor.
func TestCoordinatorClientPushXComMapIndex(t *testing.T) {
	mapped := -1
	zero := 0
	dynamic := 3

	tests := []struct {
		name            string
		mapIndex        *int
		wantHasMapIndex bool
		wantMapIndexVal int
	}{
		{name: "nil map_index is omitted", mapIndex: nil, wantHasMapIndex: false},
		{name: "-1 map_index is omitted", mapIndex: &mapped, wantHasMapIndex: false},
		{
			// Regression: map_index 0 is a real mapped index, not "unset". A plain
			// deref into the interface{} field would let omitempty drop it.
			name:            "map_index 0 is sent",
			mapIndex:        &zero,
			wantHasMapIndex: true,
			wantMapIndexVal: 0,
		},
		{
			name:            "non-negative map_index is sent",
			mapIndex:        &dynamic,
			wantHasMapIndex: true,
			wantMapIndexVal: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, map[string]any{"type": "OKResponse"}, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			ti := sdk.TaskInstance{
				DagID:    "d",
				RunID:    "r",
				TaskID:   "t",
				MapIndex: tc.mapIndex,
			}
			require.NoError(t, client.PushXCom(context.Background(), ti, "k", "v"))

			sent, err := readFrame(&requestBuf)
			require.NoError(t, err)
			sentMap := rawToMap(t, sent.Body)
			assert.Equal(t, "SetXCom", sentMap["type"])
			if tc.wantHasMapIndex {
				require.Contains(t, sentMap, "map_index")
				// msgpack decodes small ints as int8.
				assert.EqualValues(t, tc.wantMapIndexVal, sentMap["map_index"])
			} else {
				assert.NotContains(t, sentMap, "map_index",
					"map_index must be omitted for unmapped task instances")
			}
		})
	}
}

// TestCoordinatorClientGetXComMapIndex verifies the GetXCom frame omits
// map_index when none is supplied and propagates a real index otherwise,
// including index 0: a plain deref into the nullable interface{} field would let
// omitempty drop the 0 and the supervisor would resolve the unmapped XCom.
func TestCoordinatorClientGetXComMapIndex(t *testing.T) {
	zero := 0
	dynamic := 5

	tests := []struct {
		name            string
		mapIndex        *int
		wantHasMapIndex bool
		wantMapIndexVal int
	}{
		{name: "nil map_index is omitted", mapIndex: nil, wantHasMapIndex: false},
		{
			name:            "map_index 0 is sent",
			mapIndex:        &zero,
			wantHasMapIndex: true,
			wantMapIndexVal: 0,
		},
		{
			name:            "non-negative map_index is sent",
			mapIndex:        &dynamic,
			wantHasMapIndex: true,
			wantMapIndexVal: 5,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, map[string]any{
				"type":  "XComResult",
				"key":   "k",
				"value": "v",
			}, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			_, err := client.GetXCom(context.Background(), "d", "r", "t", tc.mapIndex, "k", nil)
			require.NoError(t, err)

			sent, err := readFrame(&requestBuf)
			require.NoError(t, err)
			sentMap := rawToMap(t, sent.Body)
			assert.Equal(t, "GetXCom", sentMap["type"])
			if tc.wantHasMapIndex {
				require.Contains(t, sentMap, "map_index")
				assert.EqualValues(t, tc.wantMapIndexVal, sentMap["map_index"])
			} else {
				assert.NotContains(t, sentMap, "map_index",
					"map_index must be omitted when no index is supplied")
			}
		})
	}
}

// Only an absent value may fall back; a malformed one must fail as Python's
// TaskStateStoreAccessor.set does rather than silently use the shipped default.
func TestResolveDefaultExpiry(t *testing.T) {
	now := time.Date(2026, 6, 9, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		env     string
		unset   bool
		want    any
		wantErr string
	}{
		{name: "unset falls back", unset: true, want: now.UTC().AddDate(0, 0, 30)},
		{name: "honours supervisor value", env: "7", want: now.UTC().AddDate(0, 0, 7)},
		{name: "zero days never expires", env: "0", want: nil},
		// Python's config parser accepts a whole-number float spelling.
		{name: "whole float accepted", env: "7.0", want: now.UTC().AddDate(0, 0, 7)},
		{name: "unparsable is an error", env: "abc", wantErr: "failed to convert value to int"},
		{name: "fractional is an error", env: "7.5", wantErr: "failed to convert value to int"},
		{name: "negative is an error", env: "-1", wantErr: "must be >= 0, got -1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Setenv registers the restore; only a truly unset variable falls back.
			t.Setenv(defaultRetentionDaysEnv, tc.env)
			if tc.unset {
				require.NoError(t, os.Unsetenv(defaultRetentionDaysEnv))
			}

			got, err := resolveDefaultExpiry(now)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				assert.Nil(t, got)
				return
			}
			require.NoError(t, err)
			if tc.want == nil {
				assert.Nil(t, got, "a nil expiry must be untyped so msgpack encodes null")
				return
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCoordinatorClientSetTaskStateRejectsMisconfiguredRetention(t *testing.T) {
	t.Setenv(defaultRetentionDaysEnv, "-1")

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	client := NewCoordinatorClient(
		NewCoordinatorComm(&bytes.Buffer{}, &requestBuf, logger),
		testTIID,
	)

	err := client.SetTaskState(context.Background(), "job_id", "app_001")

	require.ErrorContains(t, err, "must be >= 0, got -1")
	assert.Zero(t, requestBuf.Len(), "a rejected write must not reach the supervisor")
}

// Mirrors Python's test_set_datetime_raises_validation_error.
func TestCoordinatorClientSetTaskStateRejectsNonJSONValues(t *testing.T) {
	tests := []struct {
		name    string
		value   any
		wantErr string
	}{
		{
			name:    "datetime",
			value:   time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC),
			wantErr: "time.Time is not JSON representable",
		},
		{
			name:    "datetime nested in a map",
			value:   map[string]any{"watermark": time.Date(2026, 5, 15, 0, 0, 0, 0, time.UTC)},
			wantErr: "time.Time is not JSON representable",
		},
		{name: "NaN", value: math.NaN(), wantErr: "finite number"},
		{name: "Inf", value: math.Inf(1), wantErr: "finite number"},
		{name: "byte slice", value: []byte("raw"), wantErr: "[]byte is not JSON representable"},
		{name: "byte array", value: [16]byte{}, wantErr: "[]byte is not JSON representable"},
		{
			name:    "non-string map key",
			value:   map[int]string{1: "a"},
			wantErr: "map keys must be strings",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			client := NewCoordinatorClient(
				NewCoordinatorComm(&bytes.Buffer{}, &requestBuf, logger), testTIID,
			)

			err := client.SetTaskState(context.Background(), "job_id", tc.value)

			require.ErrorContains(t, err, tc.wantErr)
			assert.Zero(t, requestBuf.Len(), "a rejected write must not reach the supervisor")
		})
	}
}

func TestCoordinatorClientSetTaskStateAcceptsJSONShapes(t *testing.T) {
	type checkpoint struct {
		Processed int      `msgpack:"processed"`
		Cursors   []string `msgpack:"cursors"`
	}
	type skippedTime struct {
		When time.Time `json:"-"`
		Name string    `json:"name"`
	}
	values := map[string]any{
		"struct":                    checkpoint{Processed: 3, Cursors: []string{"a"}},
		"struct skipping time.Time": skippedTime{When: time.Now(), Name: "x"},
		"nested":                    map[string]any{"rows": []any{1, "two", 3.5, true, nil}},
		"scalar":                    "plain",
	}

	for name, value := range values {
		t.Run(name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, nil, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			client := NewCoordinatorClient(
				NewCoordinatorComm(&responseBuf, &requestBuf, logger),
				testTIID,
			)

			require.NoError(t, client.SetTaskState(context.Background(), "job_id", value))
			assert.NotZero(t, requestBuf.Len())
		})
	}
}

func TestCoordinatorClientGetTaskState(t *testing.T) {
	tests := []struct {
		name  string
		value any
	}{
		{name: "scalar value", value: "abc123"},
		{name: "structured value", value: map[string]any{"cursor": "abc", "done": true}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, map[string]any{
				"type":  "TaskStateStoreResult",
				"value": tc.value,
			}, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			got, err := client.GetTaskState(context.Background(), "job_id")
			require.NoError(t, err)
			assert.Equal(t, tc.value, got)

			sent, err := readFrame(&requestBuf)
			require.NoError(t, err)
			assert.Equal(t, map[string]any{
				"type":  "GetTaskStateStore",
				"ti_id": testTIID,
				"key":   "job_id",
			}, rawToMap(t, sent.Body))
		})
	}
}

func TestCoordinatorClientGetTaskStateNotFound(t *testing.T) {
	responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
		"type":   "ErrorResponse",
		"error":  "TASK_STORE_NOT_FOUND",
		"detail": map[string]any{"msg": "no such key"},
	})
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
	client := NewCoordinatorClient(comm, testTIID)

	_, err := client.GetTaskState(context.Background(), "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, sdk.TaskStateNotFound)
	assert.Contains(t, err.Error(), "missing")
}

func TestCoordinatorClientGetTaskStateErrorPassThrough(t *testing.T) {
	responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
		"type":   "ErrorResponse",
		"error":  "API_SERVER_ERROR",
		"detail": map[string]any{"msg": "boom"},
	})
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
	client := NewCoordinatorClient(comm, testTIID)

	_, err := client.GetTaskState(context.Background(), "job_id")
	require.Error(t, err)
	assert.False(t, errors.Is(err, sdk.TaskStateNotFound),
		"generic supervisor errors must not be translated to TaskStateNotFound")
	var apiErr *ApiError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, "API_SERVER_ERROR", apiErr.Err)
}

func TestCoordinatorClientUnmarshalJSONTaskState(t *testing.T) {
	type checkpoint struct {
		Cursor string `json:"cursor"`
		Done   bool   `json:"done"`
	}

	t.Run("decodes into a struct", func(t *testing.T) {
		responsePayload := encodeResponseFrame(t, 0, map[string]any{
			"type":  "TaskStateStoreResult",
			"value": map[string]any{"cursor": "abc", "done": true},
		}, nil)
		var responseBuf bytes.Buffer
		require.NoError(t, writeFrame(&responseBuf, responsePayload))

		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
		client := NewCoordinatorClient(comm, testTIID)

		var got checkpoint
		require.NoError(t, client.UnmarshalJSONTaskState(context.Background(), "job_id", &got))
		assert.Equal(t, checkpoint{Cursor: "abc", Done: true}, got)
	})

	t.Run("propagates not found", func(t *testing.T) {
		responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
			"type":   "ErrorResponse",
			"error":  "TASK_STORE_NOT_FOUND",
			"detail": map[string]any{"msg": "no such key"},
		})
		var responseBuf bytes.Buffer
		require.NoError(t, writeFrame(&responseBuf, responsePayload))

		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
		client := NewCoordinatorClient(comm, testTIID)

		var got checkpoint
		err := client.UnmarshalJSONTaskState(context.Background(), "missing", &got)
		assert.ErrorIs(t, err, sdk.TaskStateNotFound)
	})
}

// expires_at is sent even when null: the supervisor requires the field.
func TestCoordinatorClientSetTaskState(t *testing.T) {
	tests := []struct {
		name           string
		retentionDays  string
		wantExpiresNil bool
	}{
		{name: "deployment retention is applied", retentionDays: "7"},
		{name: "zero retention sends null", retentionDays: "0", wantExpiresNil: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(defaultRetentionDaysEnv, tc.retentionDays)

			responsePayload := encodeResponseFrame(t, 0, map[string]any{"type": "OKResponse"}, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			require.NoError(t, client.SetTaskState(context.Background(), "job_id", "abc123"))

			sent, err := readFrame(&requestBuf)
			require.NoError(t, err)
			sentMap := rawToMap(t, sent.Body)
			assert.Equal(t, "SetTaskStateStore", sentMap["type"])
			assert.Equal(t, testTIID, sentMap["ti_id"])
			assert.Equal(t, "job_id", sentMap["key"])
			assert.Equal(t, "abc123", sentMap["value"])
			require.Contains(t, sentMap, "expires_at",
				"expires_at must be present even when null")
			if tc.wantExpiresNil {
				assert.Nil(t, sentMap["expires_at"])
			} else {
				assert.NotNil(t, sentMap["expires_at"])
			}
		})
	}
}

func TestCoordinatorClientSetTaskStateWithRetention(t *testing.T) {
	tests := []struct {
		name           string
		retention      time.Duration
		wantErr        bool
		wantExpiresNil bool
	}{
		{name: "positive retention is sent", retention: time.Hour},
		{name: "NeverExpire sends null", retention: sdk.NeverExpire, wantExpiresNil: true},
		{name: "zero retention is rejected", retention: 0, wantErr: true},
		{name: "negative retention is rejected", retention: -time.Hour, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, map[string]any{"type": "OKResponse"}, nil)
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			err := client.SetTaskStateWithRetention(
				context.Background(), "job_id", "abc123", tc.retention,
			)
			if tc.wantErr {
				require.Error(t, err)
				assert.Zero(t, requestBuf.Len(), "a rejected retention must send no frame")
				return
			}
			require.NoError(t, err)

			sent, err := readFrame(&requestBuf)
			require.NoError(t, err)
			sentMap := rawToMap(t, sent.Body)
			assert.Equal(t, "SetTaskStateStore", sentMap["type"])
			assert.Equal(t, testTIID, sentMap["ti_id"])
			require.Contains(t, sentMap, "expires_at")
			if tc.wantExpiresNil {
				assert.Nil(t, sentMap["expires_at"])
			} else {
				assert.NotNil(t, sentMap["expires_at"])
			}
		})
	}
}

func TestCoordinatorClientSetTaskStateRejectsNilValue(t *testing.T) {
	tests := []struct {
		name string
		call func(client *CoordinatorClient) error
	}{
		{
			name: "SetTaskState",
			call: func(client *CoordinatorClient) error {
				return client.SetTaskState(context.Background(), "job_id", nil)
			},
		},
		{
			name: "SetTaskStateWithRetention",
			call: func(client *CoordinatorClient) error {
				return client.SetTaskStateWithRetention(
					context.Background(), "job_id", nil, time.Hour,
				)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var requestBuf bytes.Buffer
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(bytes.NewReader(nil), &requestBuf, logger)
			client := NewCoordinatorClient(comm, testTIID)

			require.Error(t, tc.call(client))
			assert.Zero(t, requestBuf.Len(), "a nil value must send no frame")
		})
	}
}

func TestCoordinatorClientDeleteTaskState(t *testing.T) {
	responsePayload := encodeResponseFrame(
		t,
		0,
		map[string]any{"type": "OKResponse", "ok": true},
		nil,
	)
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
	client := NewCoordinatorClient(comm, testTIID)

	require.NoError(t, client.DeleteTaskState(context.Background(), "job_id"))

	sent, err := readFrame(&requestBuf)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"type":  "DeleteTaskStateStore",
		"ti_id": testTIID,
		"key":   "job_id",
	}, rawToMap(t, sent.Body))
}

func TestCoordinatorClientClearTaskState(t *testing.T) {
	responsePayload := encodeResponseFrame(
		t,
		0,
		map[string]any{"type": "OKResponse", "ok": true},
		nil,
	)
	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)
	client := NewCoordinatorClient(comm, testTIID)

	require.NoError(t, client.ClearTaskState(context.Background()))

	sent, err := readFrame(&requestBuf)
	require.NoError(t, err)
	sentMap := rawToMap(t, sent.Body)
	assert.Equal(t, map[string]any{
		"type":  "ClearTaskStateStore",
		"ti_id": testTIID,
	}, sentMap)
	assert.NotContains(t, sentMap, "key")
}

func TestCoordinatorClientTaskStateWriteErrors(t *testing.T) {
	tests := []struct {
		name string
		call func(client *CoordinatorClient) error
	}{
		{
			name: "SetTaskState",
			call: func(client *CoordinatorClient) error {
				return client.SetTaskState(context.Background(), "job_id", "v")
			},
		},
		{
			name: "SetTaskStateWithRetention",
			call: func(client *CoordinatorClient) error {
				return client.SetTaskStateWithRetention(
					context.Background(), "job_id", "v", time.Hour,
				)
			},
		},
		{
			name: "DeleteTaskState",
			call: func(client *CoordinatorClient) error {
				return client.DeleteTaskState(context.Background(), "job_id")
			},
		},
		{
			name: "ClearTaskState",
			call: func(client *CoordinatorClient) error {
				return client.ClearTaskState(context.Background())
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
				"type":   "ErrorResponse",
				"error":  "API_SERVER_ERROR",
				"detail": map[string]any{"status_code": 403},
			})
			var responseBuf bytes.Buffer
			require.NoError(t, writeFrame(&responseBuf, responsePayload))

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			comm := NewCoordinatorComm(&responseBuf, io.Discard, logger)
			client := NewCoordinatorClient(comm, testTIID)

			var apiErr *ApiError
			require.ErrorAs(t, tc.call(client), &apiErr)
			assert.Equal(t, "API_SERVER_ERROR", apiErr.Err)
		})
	}
}

// assertNoReadReader fails the test on any Read call.
type assertNoReadReader struct{ t *testing.T }

func (f assertNoReadReader) Read(p []byte) (int, error) {
	f.t.Helper()
	f.t.Fatalf("unexpected Read on comm socket: env override should have short-circuited")
	return 0, nil
}

// assertNoWriteWriter fails the test on any Write call.
type assertNoWriteWriter struct{ t *testing.T }

func (f assertNoWriteWriter) Write(p []byte) (int, error) {
	f.t.Helper()
	f.t.Fatalf("unexpected Write on comm socket: env override should have short-circuited")
	return 0, nil
}
