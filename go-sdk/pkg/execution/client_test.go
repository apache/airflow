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
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	client := NewCoordinatorClient(comm, nil)

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
	client := NewCoordinatorClient(comm, nil)

	val, err := client.GetVariable(context.Background(), "my_key")
	require.NoError(t, err)
	assert.Equal(t, "supervisor_value", val)
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
