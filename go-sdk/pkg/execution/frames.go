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

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// MaxFrameSize is the maximum payload length of a single frame, in bytes.
// The 4-byte length prefix bounds this to 2^32 - 1; matches Python's cap in
// task-sdk comms.py:_FrameMixin.as_bytes (n >= 2**32 raises OverflowError).
const MaxFrameSize = 1<<32 - 1

// IncomingFrame represents a decoded frame received from the comm socket.
//
// Body and Err hold the raw msgpack bytes of the second and (optional) third
// array elements: the shape depends on the message's "type" discriminator, so
// callers decode them into the matching genmodels type. Err is set only for
// response frames (3-element arrays) and may itself encode a msgpack nil, which
// isNilRaw reports.
//
// ID is int64 to match the wire encoding and CoordinatorComm.nextID; narrowing
// to int would reintroduce wraparound on 32-bit GOARCH.
type IncomingFrame struct {
	ID   int64
	Body msgpack.RawMessage
	Err  msgpack.RawMessage
}

// encodeRequest encodes a request frame (2-element msgpack array: [id, body]).
// body is any msgpack-encodable value, in practice a genmodels message struct
// whose msgpack tags match the wire-schema field names.
//
// It runs body through genmodels.EnsureType so the "type" discriminator is
// stamped from its Go type at this single outbound chokepoint, not by hand at
// every call site.
func encodeRequest(id int64, body any) ([]byte, error) {
	body = genmodels.EnsureType(body)

	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	if err := enc.EncodeArrayLen(2); err != nil {
		return nil, err
	}
	if err := enc.EncodeInt(id); err != nil {
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

// decodeFrame decodes a msgpack payload into an IncomingFrame, capturing the
// body and optional error elements as raw msgpack bytes for later typed decoding.
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

	body, err := dec.DecodeRaw()
	if err != nil {
		return IncomingFrame{}, fmt.Errorf("decoding body: %w", err)
	}

	// For response frames (3-element), capture the error element.
	var errRaw msgpack.RawMessage
	if arrLen >= 3 {
		errRaw, err = dec.DecodeRaw()
		if err != nil {
			return IncomingFrame{}, fmt.Errorf("decoding error element: %w", err)
		}
	}

	return IncomingFrame{
		ID:   id64,
		Body: body,
		Err:  errRaw,
	}, nil
}

// isNilRaw reports whether a raw msgpack element is absent or encodes a msgpack
// nil (0xc0). A null error slot decodes to a single-byte nil rather than an
// empty RawMessage, so both must count as "no value".
func isNilRaw(raw msgpack.RawMessage) bool {
	return len(raw) == 0 || (len(raw) == 1 && raw[0] == 0xc0)
}
