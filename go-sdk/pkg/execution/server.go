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

// Package execution implements the SDK coordinator-protocol runtime
// (msgpack-over-IPC). It is the second mode of bundlev1server.Serve: when
// the bundle binary is launched with --comm/--logs by the Airflow supervisor
// (Python ExecutableCoordinator), bundlev1server.Serve dispatches here.
//
// The first inbound frame on the comm socket selects between two
// sub-protocols:
//
//   - DagFileParseRequest: one-shot, returns DagFileParsingResult and exits.
//   - StartupDetails:       multi-round task execution.
//
// See go-sdk/adr/0003-coordinator-protocol-msgpack-ipc.md.
package execution

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
)

// dialTimeout bounds how long execution.Serve waits to reach the supervisor's
// comm and logs sockets. The supervisor opens the listeners before spawning
// the bundle binary, so the dials normally succeed in milliseconds; the
// timeout exists so an unreachable address fails fast instead of hanging the
// runtime indefinitely.
const dialTimeout = 30 * time.Second

// Serve runs the bundle binary in coordinator mode. It dials the supervisor's
// comm and logs sockets, installs an slog handler that writes JSON-line
// records to the logs connection, and dispatches on the first frame.
//
// Serve returns nil on a clean shutdown (one-shot DAG parse or task execution
// completed); a non-nil error indicates a protocol-level failure (connection
// loss, malformed frames, unknown first message type).
func Serve(provider bundlev1.BundleProvider, commAddr, logsAddr string) error {
	if commAddr == "" {
		return fmt.Errorf("missing --comm=host:port argument")
	}
	if logsAddr == "" {
		return fmt.Errorf("missing --logs=host:port argument")
	}

	// Buffer log records until the logs socket is connected. Anything the
	// runtime emits between Connect-time and the first frame still gets
	// flushed.
	logHandler := NewSocketLogHandler(nil, slog.LevelDebug)
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	// Connect to both sockets concurrently so the supervisor can accept them
	// in either order.
	dialer := &net.Dialer{Timeout: dialTimeout}
	var commConn, logsConn net.Conn
	var commErr, logsErr error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		commConn, commErr = dialer.Dial("tcp", commAddr)
	}()
	go func() {
		defer wg.Done()
		logsConn, logsErr = dialer.Dial("tcp", logsAddr)
	}()
	wg.Wait()

	if commErr != nil {
		return fmt.Errorf("connecting to comm socket %s: %w", commAddr, commErr)
	}
	defer commConn.Close()
	if logsErr != nil {
		return fmt.Errorf("connecting to logs socket %s: %w", logsAddr, logsErr)
	}
	defer logsConn.Close()

	logHandler.Connect(logsConn)
	logger.Debug("Connected", "comm", commAddr, "logs", logsAddr)

	// Materialise the bundle (RegisterDags) up front. Both protocol paths
	// need the registry, and doing it once before the first frame keeps the
	// dispatcher simple.
	bundle, err := materialiseBundle(provider)
	if err != nil {
		return fmt.Errorf("registering dags: %w", err)
	}

	comm := NewCoordinatorComm(commConn, commConn, logger)

	frame, err := comm.ReadMessage()
	if err != nil {
		return fmt.Errorf("reading initial message: %w", err)
	}

	if frame.Err != nil {
		errResp := decodeErrorResponse(frame.Err)
		if errResp != nil {
			return fmt.Errorf(
				"received error from supervisor: [%s] %v",
				errResp.Error,
				errResp.Detail,
			)
		}
	}

	body, err := decodeIncomingBody(frame.Body)
	if err != nil {
		return fmt.Errorf("decoding initial message: %w", err)
	}

	switch msg := body.(type) {
	case *DagFileParseRequest:
		logger.Debug("DAG parsing mode", "file", msg.File)
		result := ParseDags(bundle, msg)
		if err := comm.SendRequest(frame.ID, result); err != nil {
			return fmt.Errorf("sending parse result: %w", err)
		}
		logger.Debug("DAG parsing complete")

	case *StartupDetails:
		logger.Debug("Task execution mode",
			"dag_id", msg.TI.DagID,
			"task_id", msg.TI.TaskID,
		)
		result := RunTask(bundle, msg, comm, logger)
		if err := comm.SendRequest(frame.ID, result); err != nil {
			return fmt.Errorf("sending task result: %w", err)
		}
		logger.Debug("Task execution complete")

	default:
		return fmt.Errorf("unexpected initial message type: %T", body)
	}

	return nil
}

func materialiseBundle(provider bundlev1.BundleProvider) (bundlev1.Bundle, error) {
	reg := bundlev1.New()
	if err := provider.RegisterDags(reg); err != nil {
		return nil, err
	}
	return reg, nil
}
