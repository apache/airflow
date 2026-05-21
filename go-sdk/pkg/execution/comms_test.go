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
	"sync"
	"testing"
	"time"

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

// TestCoordinatorCommCommunicateConcurrentOutOfOrder exercises the dispatcher:
// it fires Communicate from N goroutines concurrently, then has a mock
// supervisor respond in reverse order. Each caller must receive the response
// whose ID matches the request it sent, even though responses arrive in a
// different order than requests.
func TestCoordinatorCommCommunicateConcurrentOutOfOrder(t *testing.T) {
	const N = 8

	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(respR, reqW, logger)

	// Mock supervisor: read N request frames, then write the responses back in
	// reverse order. Each response echoes the request id so the client can
	// verify the response matches the request.
	supervisorDone := make(chan error, 1)
	go func() {
		ids := make([]int, 0, N)
		for i := 0; i < N; i++ {
			frame, err := readFrame(reqR)
			if err != nil {
				supervisorDone <- err
				return
			}
			ids = append(ids, frame.ID)
		}
		for i := len(ids) - 1; i >= 0; i-- {
			payload, err := encodeRequest(ids[i], map[string]any{
				"type": "VariableResult",
				"key":  "k",
				"echo": ids[i],
			})
			if err != nil {
				supervisorDone <- err
				return
			}
			if err := writeFrame(respW, payload); err != nil {
				supervisorDone <- err
				return
			}
		}
		supervisorDone <- nil
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, N)
	echoCh := make(chan int, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := comm.Communicate(map[string]any{"type": "GetVariable"})
			if err != nil {
				errCh <- err
				return
			}
			// The "echo" field carries the request id the supervisor used; the
			// dispatcher must have routed each caller to its own response.
			echo, err := toInt(resp["echo"])
			if err != nil {
				errCh <- err
				return
			}
			echoCh <- echo
		}()
	}
	wg.Wait()
	close(errCh)
	close(echoCh)

	require.NoError(t, <-supervisorDone)
	for err := range errCh {
		require.NoError(t, err)
	}

	// Each caller must have received exactly one of the N supervisor responses,
	// with no duplicates. A broken dispatcher that delivered the same response
	// to two callers would either show up as a missing id here or, in the worst
	// case, as a deadlock that fails the test by timeout.
	seen := make(map[int]bool, N)
	for echo := range echoCh {
		require.False(
			t,
			seen[echo],
			"duplicate echo id %d — dispatcher routed one response to multiple callers",
			echo,
		)
		seen[echo] = true
	}
	require.Len(t, seen, N, "expected %d distinct echo ids, got %d", N, len(seen))
	for i := 0; i < N; i++ {
		require.True(t, seen[i], "missing echo id %d from supervisor responses", i)
	}
}

// TestCoordinatorCommCommunicateResponseIDMatch verifies that a caller blocks
// on its own ID even when the supervisor delivers another caller's response
// first. The single-mutex baseline would have returned the wrong response
// here.
func TestCoordinatorCommCommunicateResponseIDMatch(t *testing.T) {
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(respR, reqW, logger)

	// Drain requests so the writes do not block. The supervisor logic below
	// drives the response order explicitly.
	go func() {
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	results := make(chan map[string]any, 2)
	go func() {
		resp, err := comm.Communicate(map[string]any{"type": "GetVariable", "key": "first"})
		require.NoError(t, err)
		results <- resp
	}()
	// Allow the first call's request id (0) to be allocated and registered.
	time.Sleep(20 * time.Millisecond)
	go func() {
		resp, err := comm.Communicate(map[string]any{"type": "GetVariable", "key": "second"})
		require.NoError(t, err)
		results <- resp
	}()
	time.Sleep(20 * time.Millisecond)

	// Reply to the SECOND caller (id 1) first. The first caller must keep
	// waiting; only the second caller unblocks.
	payload, err := encodeRequest(1, map[string]any{
		"type": "VariableResult",
		"key":  "second",
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(respW, payload))

	select {
	case resp := <-results:
		assert.Equal(t, "second", resp["key"])
	case <-time.After(time.Second):
		t.Fatal("second caller did not receive its response")
	}

	// First caller must still be blocked.
	select {
	case <-results:
		t.Fatal("first caller unblocked before its response was delivered")
	case <-time.After(50 * time.Millisecond):
	}

	// Now reply to the first caller.
	payload, err = encodeRequest(0, map[string]any{
		"type": "VariableResult",
		"key":  "first",
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(respW, payload))

	select {
	case resp := <-results:
		assert.Equal(t, "first", resp["key"])
	case <-time.After(time.Second):
		t.Fatal("first caller did not receive its response")
	}
}

// TestCoordinatorCommCommunicateDispatcherClosed verifies that a Communicate
// caller blocked on a response returns wrapped ErrDispatcherClosed when the
// reader side of the connection is closed without delivering a response.
func TestCoordinatorCommCommunicateDispatcherClosed(t *testing.T) {
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(respR, reqW, logger)

	go func() {
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := comm.Communicate(map[string]any{"type": "GetVariable"})
		errCh <- err
	}()

	// Give Communicate time to register its waiter.
	time.Sleep(50 * time.Millisecond)

	// Close the response stream; the dispatcher's read returns io.EOF (or
	// io.ErrClosedPipe) and must fan that error out to the pending waiter.
	require.NoError(t, respW.Close())

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrDispatcherClosed)
	case <-time.After(time.Second):
		t.Fatal("Communicate did not return after dispatcher closed")
	}

	// A subsequent Communicate must short-circuit on the cached read error.
	_, err := comm.Communicate(map[string]any{"type": "GetVariable"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrDispatcherClosed)
}

// TestCoordinatorCommReadMessageAfterDispatcherStarted ensures we surface a
// clear error instead of silently racing the dispatcher for input bytes.
func TestCoordinatorCommReadMessageAfterDispatcherStarted(t *testing.T) {
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(respR, reqW, logger)

	// Drain requests so the first Communicate's write doesn't deadlock.
	go func() {
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	// First Communicate starts the dispatcher (lazily).
	go func() {
		_, _ = comm.Communicate(map[string]any{"type": "GetVariable"})
	}()
	time.Sleep(50 * time.Millisecond)

	_, err := comm.ReadMessage()
	require.Error(t, err)
	assert.Contains(
		t,
		err.Error(),
		"coordinator comm: ReadMessage cannot be used after the dispatcher has started",
	)
}

// TestCoordinatorCommUnknownIDDiscarded verifies that a frame whose ID does
// not match any pending waiter is logged and skipped rather than confusing the
// next caller.
func TestCoordinatorCommUnknownIDDiscarded(t *testing.T) {
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(respR, reqW, logger)

	go func() {
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	// Inject a stray frame with an ID nobody is waiting on. The dispatcher
	// must read this without crashing or corrupting state.
	stray, err := encodeRequest(999, map[string]any{"type": "Junk"})
	require.NoError(t, err)

	errCh := make(chan error, 1)
	respCh := make(chan map[string]any, 1)
	go func() {
		resp, err := comm.Communicate(map[string]any{"type": "GetVariable"})
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	// Let Communicate register its waiter (id 0), then send the stray frame.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, writeFrame(respW, stray))

	// Now send the real response.
	payload, err := encodeRequest(0, map[string]any{
		"type": "VariableResult",
		"key":  "ok",
	})
	require.NoError(t, err)
	require.NoError(t, writeFrame(respW, payload))

	select {
	case resp := <-respCh:
		assert.Equal(t, "ok", resp["key"])
	case err := <-errCh:
		t.Fatalf("Communicate returned error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("Communicate did not return")
	}
}
