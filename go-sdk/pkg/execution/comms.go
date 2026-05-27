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
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
)

// CoordinatorComm manages bidirectional communication with the Airflow supervisor
// over a length-prefixed msgpack socket connection.
type CoordinatorComm struct {
	reader io.Reader
	writer io.Writer
	nextID atomic.Int32
	logger *slog.Logger

	wmu sync.Mutex // serialises writes
	rmu sync.Mutex // serialises reads
}

// NewCoordinatorComm creates a new communication channel.
func NewCoordinatorComm(reader io.Reader, writer io.Writer, logger *slog.Logger) *CoordinatorComm {
	return &CoordinatorComm{
		reader: reader,
		writer: writer,
		logger: logger,
	}
}

// ReadMessage reads and decodes one frame from the comm socket.
// It returns the raw IncomingFrame with decoded map bodies.
func (c *CoordinatorComm) ReadMessage() (IncomingFrame, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	frame, err := readFrame(c.reader)
	if err != nil {
		return IncomingFrame{}, fmt.Errorf("reading frame: %w", err)
	}
	c.logger.Debug("Received frame", "id", frame.ID)
	return frame, nil
}

// SendRequest sends a request frame (2-element: [id, body]) to the supervisor.
func (c *CoordinatorComm) SendRequest(id int, body map[string]any) error {
	payload, err := encodeRequest(id, body)
	if err != nil {
		return fmt.Errorf("encoding request: %w", err)
	}
	c.logger.Debug("Sending request", "id", id)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return writeFrame(c.writer, payload)
}

// Communicate sends a request and waits for the corresponding response.
// This is a synchronous request-response: the caller sends a request and blocks
// until the next frame arrives. The protocol is single-threaded on the comm socket.
//
// If the response contains an error element, it is returned as an ApiError.
// Otherwise, the response body map is returned.
func (c *CoordinatorComm) Communicate(body map[string]any) (map[string]any, error) {
	id := int(c.nextID.Add(1) - 1)

	if err := c.SendRequest(id, body); err != nil {
		return nil, err
	}

	frame, err := c.ReadMessage()
	if err != nil {
		return nil, err
	}

	// Check for error in the response.
	if frame.Err != nil {
		errResp := decodeErrorResponse(frame.Err)
		if errResp != nil {
			return nil, &ApiError{
				Err:    errResp.Error,
				Detail: errResp.Detail,
			}
		}
	}

	// Also check if the body itself is an ErrorResponse.
	if frame.Body != nil {
		if typ, _ := frame.Body["type"].(string); typ == "ErrorResponse" {
			errResp := decodeErrorResponse(frame.Body)
			return nil, &ApiError{
				Err:    errResp.Error,
				Detail: errResp.Detail,
			}
		}
	}

	return frame.Body, nil
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
