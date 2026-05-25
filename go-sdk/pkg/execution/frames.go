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
	"fmt"
	"io"

	"github.com/vmihailenco/msgpack/v5"
)

// MaxFrameSize is the maximum payload length of a single frame, in bytes.
// The 4-byte length prefix bounds this to 2^32 - 1; matches Python's cap in
// task-sdk comms.py:_FrameMixin.as_bytes (n >= 2**32 raises OverflowError).
const MaxFrameSize = 1<<32 - 1

// IncomingFrame represents a decoded frame received from the comm socket.
type IncomingFrame struct {
	ID   int
	Body map[string]any
	Err  map[string]any // non-nil only for response frames (3-element arrays)
}

// encodeRequest encodes a request frame (2-element msgpack array: [id, body]).
func encodeRequest(id int, body map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	if err := enc.EncodeArrayLen(2); err != nil {
		return nil, err
	}
	if err := enc.EncodeInt(int64(id)); err != nil {
		return nil, err
	}
	if err := enc.Encode(body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// writeFrame writes a length-prefixed msgpack payload to the writer.
// Format: [4-byte big-endian length][payload bytes].
//
// The prefix and payload are concatenated into a single buffer and written
// in one Write call so we never leave a half-framed message on the wire if
// an io.Writer implementation does a short write between the two halves.
func writeFrame(w io.Writer, payload []byte) error {
	// Refuse to send a payload the peer would refuse to read. Without this
	// guard, lengths >= 4 GiB would silently wrap in the uint32 conversion
	// below and put a corrupt length prefix on the wire, desynchronising
	// the peer instead of failing loudly here. Mirrors the OverflowError
	// raised by task-sdk comms.py:_FrameMixin.as_bytes.
	if len(payload) > MaxFrameSize {
		return fmt.Errorf(
			"frame payload length %d exceeds max %d",
			len(payload),
			MaxFrameSize,
		)
	}
	buf := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	n, err := w.Write(buf)
	if err != nil {
		return fmt.Errorf("writing frame: %w", err)
	}
	if n < len(buf) {
		return fmt.Errorf("writing frame: %w", io.ErrShortWrite)
	}
	return nil
}

// readFrame reads one length-prefixed msgpack frame from the reader and decodes it.
func readFrame(r io.Reader) (IncomingFrame, error) {
	// Read 4-byte big-endian length prefix.
	prefix := make([]byte, 4)
	if _, err := io.ReadFull(r, prefix); err != nil {
		return IncomingFrame{}, fmt.Errorf("reading length prefix: %w", err)
	}
	payloadLen := binary.BigEndian.Uint32(prefix)
	// Reject oversized frames defensively. A non-Python sender (or a
	// MaxFrameSize lowered for memory-budget reasons) might violate the cap
	// the reader is willing to allocate, so fail loudly here rather than
	// trusting the peer.
	if payloadLen > MaxFrameSize {
		return IncomingFrame{}, fmt.Errorf(
			"frame payload length %d exceeds max %d",
			payloadLen,
			MaxFrameSize,
		)
	}
	payload := make([]byte, int(payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return IncomingFrame{}, fmt.Errorf("reading payload (%d bytes): %w", payloadLen, err)
	}

	return decodeFrame(payload)
}

// decodeFrame decodes a msgpack payload into an IncomingFrame.
func decodeFrame(data []byte) (IncomingFrame, error) {
	dec := msgpack.NewDecoder(bytes.NewReader(data))

	arrLen, err := dec.DecodeArrayLen()
	if err != nil {
		return IncomingFrame{}, fmt.Errorf("decoding array header: %w", err)
	}
	if arrLen < 2 {
		return IncomingFrame{}, fmt.Errorf("unexpected frame arity %d, need at least 2", arrLen)
	}

	id64, err := dec.DecodeInt64()
	if err != nil {
		return IncomingFrame{}, fmt.Errorf("decoding frame id: %w", err)
	}

	// Decode the body element.
	bodyRaw, err := dec.DecodeInterface()
	if err != nil {
		return IncomingFrame{}, fmt.Errorf("decoding body: %w", err)
	}
	body, ok := toStringMap(bodyRaw)
	if bodyRaw != nil && !ok {
		return IncomingFrame{}, fmt.Errorf("body element: expected map, got %T", bodyRaw)
	}

	// For response frames (3-element), decode the error element.
	var errMap map[string]any
	if arrLen >= 3 {
		errRaw, err := dec.DecodeInterface()
		if err != nil {
			return IncomingFrame{}, fmt.Errorf("decoding error element: %w", err)
		}
		errMap, ok = toStringMap(errRaw)
		if errRaw != nil && !ok {
			return IncomingFrame{}, fmt.Errorf("error element: expected map, got %T", errRaw)
		}
	}

	return IncomingFrame{
		ID:   int(id64),
		Body: body,
		Err:  errMap,
	}, nil
}

// toStringMap converts a decoded interface{} to map[string]any.
// Returns nil, false if the input is nil or not a map.
func toStringMap(v any) (map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	switch m := v.(type) {
	case map[string]any:
		return m, true
	case map[any]any:
		result := make(map[string]any, len(m))
		for k, val := range m {
			result[fmt.Sprint(k)] = val
		}
		return result, true
	default:
		return nil, false
	}
}

// mapString extracts a string value from a map.
func mapString(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", fmt.Errorf("missing key %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("key %q: expected string, got %T", key, v)
	}
	return s, nil
}

// mapIntOr extracts an int value from a map, returning the default if missing.
func mapIntOr(m map[string]any, key string, def int) int {
	v, ok := m[key]
	if !ok {
		return def
	}
	n, err := toInt(v)
	if err != nil {
		return def
	}
	return n
}

// mapStringOr extracts a string value from a map, returning the default if missing.
func mapStringOr(m map[string]any, key string, def string) string {
	v, ok := m[key]
	if !ok {
		return def
	}
	s, ok := v.(string)
	if !ok {
		return def
	}
	return s
}

// mapMap extracts a nested map from a map.
func mapMap(m map[string]any, key string) map[string]any {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	sub, ok := toStringMap(v)
	if !ok {
		return nil
	}
	return sub
}

// toInt converts various numeric types from msgpack decoding to int.
func toInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int8:
		return int(n), nil
	case int16:
		return int(n), nil
	case int32:
		return int(n), nil
	case int64:
		return int(n), nil
	case uint:
		return int(n), nil
	case uint8:
		return int(n), nil
	case uint16:
		return int(n), nil
	case uint32:
		return int(n), nil
	case uint64:
		return int(n), nil
	case float32:
		return int(n), nil
	case float64:
		return int(n), nil
	default:
		return 0, fmt.Errorf("expected numeric, got %T", v)
	}
}
