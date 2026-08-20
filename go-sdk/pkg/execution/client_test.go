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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/sdk"
)

// TestCoordinatorClientGetVariableEnvOverride verifies that an
// AIRFLOW_VAR_<UPPER(key)> environment override short-circuits the comm
// socket, matching the HTTP-backed sdk.client behaviour.
func TestCoordinatorClientGetVariableEnvOverride(t *testing.T) {
	t.Setenv("AIRFLOW_VAR_MY_KEY", "env_value")

	// A nil reader would panic if the dispatcher ever read; an empty
	// io.Discard write target would silently accept any request. Use both
	// to assert *no* IO occurred by failing if anything is read or written.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(assertNoReadReader{t: t}, assertNoWriteWriter{t: t}, logger)
	client := NewCoordinatorClient(comm)

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
	client := NewCoordinatorClient(comm)

	val, err := client.GetVariable(context.Background(), "my_key")
	require.NoError(t, err)
	assert.Equal(t, "supervisor_value", val)
}

// TestCoordinatorClientErrorTranslation verifies that GetVariable,
// GetConnection, and GetXCom translate the supervisor's *_NOT_FOUND error
// codes into the SDK sentinel errors so call-site `errors.Is` checks behave
// the same in coordinator and HTTP mode.
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
			client := NewCoordinatorClient(comm)

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
	client := NewCoordinatorClient(comm)

	_, err := client.GetVariable(context.Background(), "any_key")
	require.Error(t, err)
	assert.False(t, errors.Is(err, sdk.VariableNotFound),
		"generic supervisor errors must not be translated to VariableNotFound")
	var apiErr *ApiError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, "API_SERVER_ERROR", apiErr.Err)
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
	client := NewCoordinatorClient(comm)

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
	client := NewCoordinatorClient(comm)

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
			client := NewCoordinatorClient(comm)

			ti := api.TaskInstance{
				DagId:    "d",
				RunId:    "r",
				TaskId:   "t",
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
			client := NewCoordinatorClient(comm)

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
