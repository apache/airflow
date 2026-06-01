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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
)

// CoordinatorComm manages bidirectional communication with the Airflow
// supervisor over a length-prefixed msgpack socket connection.
//
// Reads are multiplexed by frame ID. A single background reader goroutine,
// lazily started on the first Communicate call, consumes inbound frames and
// dispatches each one to the Communicate caller whose request used that ID.
// This lets multiple goroutines call Communicate concurrently without
// serialising the full send-then-read round trip behind a single mutex, and
// guarantees the response a caller receives matches the request it sent.
//
// The supervisor's initial StartupDetails frame arrives unsolicited, before
// any client request is in flight, and is read synchronously via
// ReadMessage; ReadMessage must not be called after the dispatcher has been
// started.
type CoordinatorComm struct {
	reader io.Reader
	writer io.Writer
	nextID atomic.Int64
	logger *slog.Logger

	wmu sync.Mutex // serialises writes

	// Multiplexer state. mu protects every field below.
	mu      sync.Mutex
	pending map[int64]chan frameResult
	started bool
	readErr error
}

// frameResult is the value the dispatcher delivers to a Communicate caller.
type frameResult struct {
	frame IncomingFrame
	err   error
}

// ErrDispatcherClosed is wrapped into the error Communicate returns once the
// background reader goroutine has exited — typically because the supervisor
// closed the comm socket.
var ErrDispatcherClosed = errors.New("coordinator comm: dispatcher closed")

// NewCoordinatorComm creates a new communication channel.
func NewCoordinatorComm(reader io.Reader, writer io.Writer, logger *slog.Logger) *CoordinatorComm {
	return &CoordinatorComm{
		reader:  reader,
		writer:  writer,
		logger:  logger,
		pending: make(map[int64]chan frameResult),
	}
}

// ReadMessage reads and decodes one frame directly from the comm socket.
// It is used to read the supervisor's initial frame before any request/response
// traffic begins. Calling it after the dispatcher has started would race the
// reader goroutine for input bytes, so it returns an error in that case.
func (c *CoordinatorComm) ReadMessage() (IncomingFrame, error) {
	c.mu.Lock()
	started := c.started
	c.mu.Unlock()
	if started {
		return IncomingFrame{}, errors.New(
			"coordinator comm: ReadMessage cannot be used after the dispatcher has started",
		)
	}

	frame, err := readFrame(c.reader)
	if err != nil {
		return IncomingFrame{}, fmt.Errorf("reading frame: %w", err)
	}
	c.logger.Debug("Received frame", "id", frame.ID)
	return frame, nil
}

// SendRequest writes a request frame (2-element [id, body]) to the supervisor.
// Concurrent calls are serialised so frames are never interleaved on the wire.
func (c *CoordinatorComm) SendRequest(id int64, body map[string]any) error {
	payload, err := encodeRequest(id, body)
	if err != nil {
		return fmt.Errorf("encoding request: %w", err)
	}
	c.logger.Debug("Sending request", "id", id)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.writer, payload)
}

// Communicate sends a request and blocks until the supervisor's response with
// the matching frame ID is delivered by the dispatcher, ctx is cancelled, or
// its deadline expires. Safe to call concurrently from multiple goroutines.
//
// If the response carries an error (either as the third element of a 3-tuple
// frame or as a body whose "type" is "ErrorResponse") it is returned as an
// *ApiError. If the dispatcher's read loop has terminated, the underlying read
// error is returned wrapped in ErrDispatcherClosed.
func (c *CoordinatorComm) Communicate(
	ctx context.Context,
	body map[string]any,
) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	id := c.nextID.Add(1) - 1
	ch := make(chan frameResult, 1)

	// Register the waiter under the same lock the dispatcher uses, and before
	// sending the request, so the dispatcher can never deliver the response
	// (or a terminal read error) before this caller is ready to receive it.
	c.mu.Lock()
	if !c.started {
		c.started = true
		go c.readLoop()
	}
	if c.readErr != nil {
		err := c.readErr
		c.mu.Unlock()
		return nil, fmt.Errorf("%w: %w", ErrDispatcherClosed, err)
	}
	c.pending[id] = ch
	c.mu.Unlock()

	// Run the send in a goroutine so ctx cancellation can interrupt a blocked
	// write. We deliberately do not touch the underlying connection on cancel
	// (no SetWriteDeadline, no Close): manipulating the stream mid-write would
	// either poison future writes with a stale deadline or risk a partial
	// length-prefixed frame on the wire. Instead, we let the send goroutine
	// run to completion in the background. If it eventually succeeds the
	// supervisor's response has no waiter and is discarded by the dispatcher;
	// if it fails the dispatcher will surface the error to other callers.
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- c.SendRequest(id, body)
	}()

	select {
	case err := <-sendErr:
		if err != nil {
			c.mu.Lock()
			delete(c.pending, id)
			c.mu.Unlock()
			return nil, err
		}
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}

	var result frameResult
	select {
	case result = <-ch:
	case <-ctx.Done():
		// Drop the waiter so a late supervisor reply is logged-and-discarded
		// by the dispatcher rather than piling up in c.pending. The dispatcher
		// may have already delivered the response into the buffered channel
		// between the select and this delete; that delivery is harmless (the
		// channel is buffered to 1 and the caller has already given up).
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return nil, ctx.Err()
	}
	if result.err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDispatcherClosed, result.err)
	}
	frame := result.frame

	// An error reply can arrive in two shapes: the third element of a 3-tuple
	// response frame, or the body of a 2-tuple frame whose "type" is
	// "ErrorResponse". Pick whichever the supervisor used and decode once.
	if errMap := errMapFromFrame(frame); errMap != nil {
		errResp := decodeErrorResponse(errMap)
		return nil, &ApiError{
			Err:    errResp.Error,
			Detail: errResp.Detail,
		}
	}

	return frame.Body, nil
}

// errMapFromFrame returns the error-shaped map carried by a response frame, or
// nil if the frame is not an error reply. The supervisor may surface an error
// either as the dedicated third element of a 3-tuple frame (frame.Err) or as
// the body of a 2-tuple frame whose "type" is "ErrorResponse".
func errMapFromFrame(f IncomingFrame) map[string]any {
	if f.Err != nil {
		return f.Err
	}
	if f.Body != nil {
		if typ, _ := f.Body["type"].(string); typ == "ErrorResponse" {
			return f.Body
		}
	}
	return nil
}

// readLoop is the dispatcher: it reads frames from the comm socket and routes
// each one to the channel registered by the matching Communicate caller. When
// readFrame returns an error (typically io.EOF on supervisor shutdown), it
// fans the error out to every pending waiter and exits; any subsequent
// Communicate call sees the error via readErr and returns immediately.
func (c *CoordinatorComm) readLoop() {
	for {
		frame, err := readFrame(c.reader)
		if err != nil {
			c.mu.Lock()
			c.readErr = err
			pending := c.pending
			c.pending = nil
			c.mu.Unlock()
			for _, ch := range pending {
				ch <- frameResult{err: err}
			}
			return
		}
		c.logger.Debug("Received frame", "id", frame.ID)

		c.mu.Lock()
		ch, ok := c.pending[frame.ID]
		if ok {
			delete(c.pending, frame.ID)
		}
		c.mu.Unlock()
		if !ok {
			// A waiter whose caller cancelled or timed out is deliberately
			// removed by Communicate, leaving its late reply to land here. That
			// is the expected deadline path, not a protocol bug, so log at Debug.
			c.logger.Debug("Discarding frame with no matching waiter", "id", frame.ID)
			continue
		}
		ch <- frameResult{frame: frame}
	}
}

// ApiError represents an error returned by the supervisor over the comm socket.
type ApiError struct {
	Err    string
	Detail any
}

func (e *ApiError) Error() string {
	if e.Detail != nil {
		return fmt.Sprintf("[%s] %v", e.Err, e.Detail)
	}
	return e.Err
}
