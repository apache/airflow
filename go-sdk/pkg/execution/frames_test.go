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

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
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

// TestEncodeRequestStampsType verifies encodeRequest stamps the "type"
// discriminator from the body's Go type (via genmodels.EnsureType), so a call
// site that leaves Type unset — or sets the wrong one — still produces a frame
// whose discriminator matches the model. This is the guarantee that keeps the
// type<->model binding from drifting.
func TestEncodeRequestStampsType(t *testing.T) {
	t.Run("unset Type is stamped", func(t *testing.T) {
		data, err := encodeRequest(1, genmodels.GetConnection{ConnID: "c"})
		require.NoError(t, err)
		frame, err := decodeFrame(data)
		require.NoError(t, err)
		assert.Equal(t, genmodels.TypeGetConnection, peekBodyType(frame.Body))
	})

	t.Run("wrong Type is corrected", func(t *testing.T) {
		data, err := encodeRequest(
			2,
			genmodels.SetXCom{Type: genmodels.TypeGetConnection, Key: "k"},
		)
		require.NoError(t, err)
		frame, err := decodeFrame(data)
		require.NoError(t, err)
		assert.Equal(t, genmodels.TypeSetXCom, peekBodyType(frame.Body))
	})
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
	assert.Equal(t, int64(7), frame.ID)
	bodyMap := rawToMap(t, frame.Body)
	assert.Equal(t, "GetConnection", bodyMap["type"])
	assert.Equal(t, "my_db", bodyMap["conn_id"])
	assert.True(t, isNilRaw(frame.Err))
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
	assert.Equal(t, int64(5), frame.ID)
	bodyMap := rawToMap(t, frame.Body)
	assert.Equal(t, "ConnectionResult", bodyMap["type"])
	assert.Equal(t, "localhost", bodyMap["host"])
	assert.True(t, isNilRaw(frame.Err))
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
	assert.Equal(t, int64(3), frame.ID)
	assert.True(t, isNilRaw(frame.Body))
	assert.False(t, isNilRaw(frame.Err))
	assert.Equal(t, "not_found", rawToMap(t, frame.Err)["error"])
}

func TestRoundTripMultipleFrames(t *testing.T) {
	var buf bytes.Buffer

	// Write two frames.
	bodies := []map[string]any{
		{"type": "GetVariable", "key": "v1"},
		{"type": "GetVariable", "key": "v2"},
	}
	for i, body := range bodies {
		payload, err := encodeRequest(int64(i), body)
		require.NoError(t, err)
		require.NoError(t, writeFrame(&buf, payload))
	}

	// Read them back.
	for i, expected := range bodies {
		frame, err := readFrame(&buf)
		require.NoError(t, err)
		assert.Equal(t, int64(i), frame.ID)
		assert.Equal(t, expected["key"], rawToMap(t, frame.Body)["key"])
	}
}
