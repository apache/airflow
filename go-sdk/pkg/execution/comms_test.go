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
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCoordinatorCommReadMessage(t *testing.T) {
	body := map[string]any{
		"type": "DagFileParseRequest",
		"file": "/path/to/dags.go",
	}
	payload, err := encodeRequest(0, body)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeFrame(&buf, payload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&buf, io.Discard, logger)

	frame, err := comm.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, 0, frame.ID)
	assert.Equal(t, "DagFileParseRequest", frame.Body["type"])
}

func TestCoordinatorCommSendRequest(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), &buf, logger)

	body := map[string]any{
		"type": "GetVariable",
		"key":  "test_key",
	}
	err := comm.SendRequest(5, body)
	require.NoError(t, err)

	frame, err := readFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, 5, frame.ID)
	assert.Equal(t, "GetVariable", frame.Body["type"])
	assert.Equal(t, "test_key", frame.Body["key"])
}

func TestCoordinatorCommCommunicate(t *testing.T) {
	// Prepare a response frame for the mock supervisor to "return".
	responseBody := map[string]any{
		"type":  "VariableResult",
		"key":   "my_var",
		"value": "my_value",
	}
	responsePayload, err := encodeRequest(0, responseBody)
	require.NoError(t, err)

	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)

	result, err := comm.Communicate(GetVariableMsg{Key: "my_var"}.toMap())
	require.NoError(t, err)
	assert.Equal(t, "VariableResult", result["type"])
	assert.Equal(t, "my_value", result["value"])

	// Verify the request was sent.
	sentFrame, err := readFrame(&requestBuf)
	require.NoError(t, err)
	assert.Equal(t, "GetVariable", sentFrame.Body["type"])
}

func TestCoordinatorCommCommunicateError(t *testing.T) {
	// Build a 3-element response frame with an error.
	responsePayload := encodeResponseFrame(t, 0, nil, map[string]any{
		"type":   "ErrorResponse",
		"error":  "not_found",
		"detail": "Variable not found",
	})

	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)

	_, err := comm.Communicate(GetVariableMsg{Key: "missing"}.toMap())
	require.Error(t, err)

	apiErr, ok := err.(*ApiError)
	require.True(t, ok)
	assert.Equal(t, "not_found", apiErr.Err)
}

func TestCoordinatorCommCommunicateBodyError(t *testing.T) {
	// Error can also come in the body element of a 2-element frame.
	errorBody := map[string]any{
		"type":   "ErrorResponse",
		"error":  "server_error",
		"detail": "internal failure",
	}
	responsePayload, err := encodeRequest(0, errorBody)
	require.NoError(t, err)

	var responseBuf bytes.Buffer
	require.NoError(t, writeFrame(&responseBuf, responsePayload))

	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&responseBuf, &requestBuf, logger)

	_, err = comm.Communicate(GetVariableMsg{Key: "test"}.toMap())
	require.Error(t, err)

	apiErr, ok := err.(*ApiError)
	require.True(t, ok)
	assert.Equal(t, "server_error", apiErr.Err)
}

func TestApiErrorFormat(t *testing.T) {
	err := &ApiError{Err: "not_found", Detail: "Variable 'x' not found"}
	assert.Equal(t, "[not_found] Variable 'x' not found", err.Error())

	err2 := &ApiError{Err: "server_error"}
	assert.Equal(t, "server_error", err2.Error())
}

// encodeResponseFrame encodes a 3-element response frame for testing.
func encodeResponseFrame(t *testing.T, id int, body, errBody map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	require.NoError(t, enc.EncodeArrayLen(3))
	require.NoError(t, enc.EncodeInt(int64(id)))
	require.NoError(t, enc.Encode(body))
	require.NoError(t, enc.Encode(errBody))
	return buf.Bytes()
}
