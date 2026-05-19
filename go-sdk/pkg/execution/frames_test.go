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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestEncodeRequest(t *testing.T) {
	body := map[string]any{
		"type": "GetVariable",
		"key":  "my_var",
	}

	data, err := encodeRequest(42, body)
	require.NoError(t, err)

	// Decode and verify structure.
	dec := msgpack.NewDecoder(bytes.NewReader(data))
	arrLen, err := dec.DecodeArrayLen()
	require.NoError(t, err)
	assert.Equal(t, 2, arrLen, "request frame should be 2-element array")

	id, err := dec.DecodeInt64()
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)

	var decodedBody map[string]any
	err = dec.Decode(&decodedBody)
	require.NoError(t, err)
	assert.Equal(t, "GetVariable", decodedBody["type"])
	assert.Equal(t, "my_var", decodedBody["key"])
}

func TestWriteAndReadFrame(t *testing.T) {
	body := map[string]any{
		"type":    "GetConnection",
		"conn_id": "my_db",
	}

	payload, err := encodeRequest(7, body)
	require.NoError(t, err)

	// Write to buffer with length prefix.
	var buf bytes.Buffer
	err = writeFrame(&buf, payload)
	require.NoError(t, err)

	// Verify length prefix.
	prefix := buf.Bytes()[:4]
	expectedLen := uint32(len(payload))
	assert.Equal(t, expectedLen, binary.BigEndian.Uint32(prefix))

	// Read back.
	frame, err := readFrame(&buf)
	require.NoError(t, err)
	assert.Equal(t, 7, frame.ID)
	assert.Equal(t, "GetConnection", frame.Body["type"])
	assert.Equal(t, "my_db", frame.Body["conn_id"])
	assert.Nil(t, frame.Err)
}

func TestDecodeResponseFrame(t *testing.T) {
	// Encode a 3-element response frame: [id, body, error]
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	require.NoError(t, enc.EncodeArrayLen(3))
	require.NoError(t, enc.EncodeInt(5))
	require.NoError(t, enc.Encode(map[string]any{
		"type":    "ConnectionResult",
		"conn_id": "test_conn",
		"host":    "localhost",
	}))
	require.NoError(t, enc.Encode(nil)) // no error

	frame, err := decodeFrame(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, 5, frame.ID)
	assert.Equal(t, "ConnectionResult", frame.Body["type"])
	assert.Equal(t, "localhost", frame.Body["host"])
	assert.Nil(t, frame.Err)
}

func TestDecodeResponseFrameWithError(t *testing.T) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	require.NoError(t, enc.EncodeArrayLen(3))
	require.NoError(t, enc.EncodeInt(3))
	require.NoError(t, enc.Encode(nil)) // nil body
	require.NoError(t, enc.Encode(map[string]any{
		"type":   "ErrorResponse",
		"error":  "not_found",
		"detail": "Variable 'x' not found",
	}))

	frame, err := decodeFrame(buf.Bytes())
	require.NoError(t, err)
	assert.Equal(t, 3, frame.ID)
	assert.Nil(t, frame.Body)
	assert.NotNil(t, frame.Err)
	assert.Equal(t, "not_found", frame.Err["error"])
}

func TestRoundTripMultipleFrames(t *testing.T) {
	var buf bytes.Buffer

	// Write two frames.
	bodies := []map[string]any{
		{"type": "GetVariable", "key": "v1"},
		{"type": "GetVariable", "key": "v2"},
	}
	for i, body := range bodies {
		payload, err := encodeRequest(i, body)
		require.NoError(t, err)
		require.NoError(t, writeFrame(&buf, payload))
	}

	// Read them back.
	for i, expected := range bodies {
		frame, err := readFrame(&buf)
		require.NoError(t, err)
		assert.Equal(t, i, frame.ID)
		assert.Equal(t, expected["key"], frame.Body["key"])
	}
}

func TestToStringMap(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  map[string]any
		ok    bool
	}{
		{"nil", nil, nil, false},
		{"string map", map[string]any{"a": 1}, map[string]any{"a": 1}, true},
		{"any key map", map[any]any{"b": 2}, map[string]any{"b": 2}, true},
		{"not a map", "hello", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := toStringMap(tt.input)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestToInt(t *testing.T) {
	tests := []struct {
		input any
		want  int
	}{
		{int8(42), 42},
		{int16(42), 42},
		{int32(42), 42},
		{int64(42), 42},
		{uint8(42), 42},
		{uint16(42), 42},
		{uint32(42), 42},
		{uint64(42), 42},
		{float32(42.0), 42},
		{float64(42.0), 42},
		{int(42), 42},
	}
	for _, tt := range tests {
		got, err := toInt(tt.input)
		require.NoError(t, err)
		assert.Equal(t, tt.want, got)
	}

	_, err := toInt("not a number")
	assert.Error(t, err)
}
