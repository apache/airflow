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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestCoordinatorCommReadMessage(t *testing.T) {
	body := map[string]any{
		"type": "StartupDetails",
		"ti": map[string]any{
			"id":      "550e8400-e29b-41d4-a716-446655440000",
			"task_id": "t", "dag_id": "d", "run_id": "r",
		},
	}
	payload, err := encodeRequest(0, body)
	require.NoError(t, err)

	var buf bytes.Buffer
	require.NoError(t, writeFrame(&buf, payload))

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(&buf, io.Discard, logger)

	frame, err := comm.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, int64(0), frame.ID)
	assert.Equal(t, "StartupDetails", frame.Body["type"])
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
	assert.Equal(t, int64(5), frame.ID)
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

	result, err := comm.Communicate(context.Background(), GetVariableMsg{Key: "my_var"}.toMap())
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

	_, err := comm.Communicate(context.Background(), GetVariableMsg{Key: "missing"}.toMap())
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

	_, err = comm.Communicate(context.Background(), GetVariableMsg{Key: "test"}.toMap())
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
func encodeResponseFrame(t *testing.T, id int64, body, errBody map[string]any) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)

	require.NoError(t, enc.EncodeArrayLen(3))
	require.NoError(t, enc.EncodeInt(id))
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
		ids := make([]int64, 0, N)
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
			resp, err := comm.Communicate(
				context.Background(),
				map[string]any{"type": "GetVariable"},
			)
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

	// The supervisor reads exactly two request frames and reports each id back
	// through reqIDs. A successful readFrame is proof the caller already
	// registered its waiter (Communicate registers before it calls
	// SendRequest), so the test can sequence id allocation deterministically
	// instead of relying on sleeps.
	reqIDs := make(chan int64, 2)
	go func() {
		for i := 0; i < 2; i++ {
			frame, err := readFrame(reqR)
			if err != nil {
				return
			}
			reqIDs <- frame.ID
		}
	}()

	results := make(chan map[string]any, 2)
	go func() {
		resp, err := comm.Communicate(
			context.Background(),
			map[string]any{"type": "GetVariable", "key": "first"},
		)
		require.NoError(t, err)
		results <- resp
	}()
	firstID := <-reqIDs
	go func() {
		resp, err := comm.Communicate(
			context.Background(),
			map[string]any{"type": "GetVariable", "key": "second"},
		)
		require.NoError(t, err)
		results <- resp
	}()
	secondID := <-reqIDs

	// Reply to the SECOND caller first. The first caller must keep waiting;
	// only the second caller unblocks.
	payload, err := encodeRequest(secondID, map[string]any{
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
	payload, err = encodeRequest(firstID, map[string]any{
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

	// registered fires once the supervisor has read the caller's request
	// frame, which guarantees Communicate has already registered its waiter
	// (registration precedes SendRequest).
	registered := make(chan struct{})
	go func() {
		if _, err := readFrame(reqR); err == nil {
			close(registered)
		}
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		_, err := comm.Communicate(context.Background(), map[string]any{"type": "GetVariable"})
		errCh <- err
	}()

	<-registered

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
	_, err := comm.Communicate(context.Background(), map[string]any{"type": "GetVariable"})
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

	// Signal once Communicate's request has reached the wire; by then the
	// dispatcher goroutine has been started (Communicate starts it before
	// SendRequest under the same lock).
	dispatcherUp := make(chan struct{})
	go func() {
		if _, err := readFrame(reqR); err == nil {
			close(dispatcherUp)
		}
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	// First Communicate starts the dispatcher (lazily).
	go func() {
		_, _ = comm.Communicate(context.Background(), map[string]any{"type": "GetVariable"})
	}()
	<-dispatcherUp

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

	// Report the caller's allocated request id as soon as the supervisor sees
	// its frame on the wire. By then Communicate has registered its waiter,
	// so we can safely send the stray frame and then the matching response.
	reqIDs := make(chan int64, 1)
	go func() {
		if frame, err := readFrame(reqR); err == nil {
			reqIDs <- frame.ID
		}
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
		resp, err := comm.Communicate(context.Background(), map[string]any{"type": "GetVariable"})
		if err != nil {
			errCh <- err
			return
		}
		respCh <- resp
	}()
	id := <-reqIDs
	require.NoError(t, writeFrame(respW, stray))

	// Now send the real response.
	payload, err := encodeRequest(id, map[string]any{
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

// failingWriter is an io.Writer that always returns the same error. Used by
// TestCoordinatorCommSendRequestErrorCleansPending to force SendRequest to
// fail after a Communicate caller has already registered its waiter.
type failingWriter struct {
	err error
}

func (w *failingWriter) Write(_ []byte) (int, error) {
	return 0, w.err
}

// TestCoordinatorCommSendRequestErrorCleansPending verifies that when
// SendRequest fails (e.g. the underlying socket has died), Communicate removes
// the waiter it registered before sending. If the cleanup path were ever
// dropped, the entry would leak in c.pending and a stale supervisor reply
// arriving on that id would silently land on a channel nobody is reading.
func TestCoordinatorCommSendRequestErrorCleansPending(t *testing.T) {
	// The reader side is a never-fed pipe; the dispatcher started by
	// Communicate parks on readFrame until t.Cleanup closes it.
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		respR.Close()
		respW.Close()
	})

	writeErr := errors.New("simulated write failure")
	comm := NewCoordinatorComm(
		respR,
		&failingWriter{err: writeErr},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	_, err := comm.Communicate(context.Background(), map[string]any{"type": "GetVariable"})
	require.Error(t, err)
	assert.ErrorIs(t, err, writeErr)

	comm.mu.Lock()
	pendingLen := len(comm.pending)
	comm.mu.Unlock()
	assert.Equal(
		t,
		0,
		pendingLen,
		"pending map should be empty after SendRequest failure, got %d entries",
		pendingLen,
	)
}

// TestCoordinatorCommCommunicateContextCancelled verifies that a Communicate
// caller whose context is cancelled while waiting for a response returns
// promptly with the context error and removes its entry from c.pending so a
// late supervisor reply lands in the dispatcher's "no matching waiter" path
// instead of leaking memory.
func TestCoordinatorCommCommunicateContextCancelled(t *testing.T) {
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

	// Drain the request side so SendRequest does not block; signal once the
	// caller's request frame has reached the wire so we know its waiter is
	// registered before we cancel ctx.
	registered := make(chan struct{})
	go func() {
		if _, err := readFrame(reqR); err == nil {
			close(registered)
		}
		for {
			if _, err := readFrame(reqR); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := comm.Communicate(ctx, map[string]any{"type": "GetVariable"})
		errCh <- err
	}()

	<-registered
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Communicate did not return after ctx was cancelled")
	}

	comm.mu.Lock()
	pendingLen := len(comm.pending)
	comm.mu.Unlock()
	assert.Equal(
		t,
		0,
		pendingLen,
		"pending map should be empty after ctx cancellation, got %d entries",
		pendingLen,
	)
}

// TestCoordinatorCommCommunicateCancelDuringSend verifies that Communicate
// returns when ctx is cancelled while the request write is still blocked.
// The caller must escape with ctx.Err() and its waiter must be cleaned up;
// the send goroutine is allowed to remain parked on the wedged writer (it
// would otherwise corrupt the stream if force-unblocked mid-frame).
func TestCoordinatorCommCommunicateCancelDuringSend(t *testing.T) {
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		respR.Close()
		respW.Close()
	})

	writer := newBlockingWriter()
	t.Cleanup(writer.release)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(respR, writer, logger)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := comm.Communicate(ctx, map[string]any{"type": "GetVariable"})
		errCh <- err
	}()

	// Wait until the goroutine has actually entered the blocked Write so the
	// cancel below targets the in-flight send, not the pre-write ctx check.
	writer.waitForWrite(t)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Communicate did not return while the request write was blocked")
	}

	comm.mu.Lock()
	pendingLen := len(comm.pending)
	comm.mu.Unlock()
	assert.Equal(t, 0, pendingLen,
		"pending map should be empty after ctx cancellation during send")
}

// blockingWriter blocks every Write call until release() is called. It is used
// to simulate a stalled coordinator socket so a cancelled Communicate has to
// unblock the caller via context handling rather than the writer completing.
type blockingWriter struct {
	once    sync.Once
	release func()
	gate    chan struct{}
	entered chan struct{}
}

func newBlockingWriter() *blockingWriter {
	w := &blockingWriter{
		gate:    make(chan struct{}),
		entered: make(chan struct{}),
	}
	w.release = func() {
		w.once.Do(func() { close(w.gate) })
	}
	return w
}

func (w *blockingWriter) Write(p []byte) (int, error) {
	select {
	case <-w.entered:
	default:
		close(w.entered)
	}
	<-w.gate
	return len(p), nil
}

func (w *blockingWriter) waitForWrite(t *testing.T) {
	t.Helper()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("blockingWriter.Write was never called")
	}
}

// TestCoordinatorCommCommunicateContextAlreadyDone verifies that a Communicate
// call whose context is already cancelled returns immediately without sending
// a request frame or starting the dispatcher.
func TestCoordinatorCommCommunicateContextAlreadyDone(t *testing.T) {
	var requestBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	comm := NewCoordinatorComm(bytes.NewReader(nil), &requestBuf, logger)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := comm.Communicate(ctx, map[string]any{"type": "GetVariable"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, requestBuf.Len(), "no request frame should have been sent")
}
