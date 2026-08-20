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
// The first inbound frame on the comm socket is a StartupDetails message
// that drives multi-round task execution.
//
// See go-sdk/adr/0003-coordinator-protocol-msgpack-ipc.md.
package execution

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// dialTimeout bounds how long execution.Serve waits to reach the supervisor's
// comm and logs sockets. The supervisor opens the listeners before spawning
// the bundle binary, so the dials normally succeed in milliseconds; the
// timeout exists so an unreachable address fails fast instead of hanging the
// runtime indefinitely.
const dialTimeout = 30 * time.Second

// terminalSendTimeout bounds the write of the final TaskState/SucceedTask
// frame. The supervisor normally drains the comm socket promptly, but a
// half-open connection (the supervisor gone without a clean close) could
// otherwise wedge the runtime on a blocked write; the deadline turns that
// into a fast failure -- and thus a non-zero exit -- instead of a hang.
const terminalSendTimeout = 30 * time.Second

// Serve runs the bundle binary in coordinator mode. It dials the supervisor's
// comm and logs sockets, installs an slog handler that writes JSON-line
// records to the logs connection, and dispatches on the first frame.
//
// Serve returns nil on a clean shutdown: the task ran and its terminal
// TaskState/SucceedTask frame was delivered, and the caller should exit 0. A
// non-nil error indicates a protocol-level failure (connection loss,
// malformed frames, unknown first message type) that happens before or
// instead of delivering a terminal frame.
//
// Failure-signaling contract: the caller (main) must turn a non-nil error
// into a non-zero process exit. The supervisor derives the task's final state
// primarily from the child's exit code -- a non-zero exit is recorded as
// FAILED (or UP_FOR_RETRY when retries are configured), and a structured
// TaskState frame is only honored when the process exits 0 (see the Python
// supervisor's ActivitySubprocess.final_state). So an early error return here
// fails closed without needing to send a frame; the post-connect paths below
// log the reason at Error first so it still reaches the supervisor's log
// stream over the already-connected logs socket.
func Serve(provider bundlev1.BundleProvider, commAddr, logsAddr string) error {
	if commAddr == "" {
		return fmt.Errorf("missing --comm=host:port argument")
	}
	if logsAddr == "" {
		return fmt.Errorf("missing --logs=host:port argument")
	}

	// A supervisor shutdown arrives as SIGTERM (escalated to SIGKILL after a
	// grace period). Trap SIGINT/SIGTERM into a context so a cooperative,
	// ctx-aware task can observe the shutdown and return promptly. A task that
	// ignores ctx is still stopped by the supervisor's follow-up SIGKILL, so
	// trapping the signal here does not strand a non-cooperative task.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	// Either dial may succeed while the other fails; close any orphaned
	// connection before returning so we don't leak an open TCP socket.
	if commErr != nil {
		if logsConn != nil {
			logsConn.Close()
		}
		return fmt.Errorf("connecting to comm socket %s: %w", commAddr, commErr)
	}
	if logsErr != nil {
		commConn.Close()
		return fmt.Errorf("connecting to logs socket %s: %w", logsAddr, logsErr)
	}
	defer commConn.Close()
	defer logsConn.Close()

	logHandler.Connect(logsConn)
	logger.Debug("Connected", "comm", commAddr, "logs", logsAddr)

	// Materialise the bundle (RegisterDags) up front. Both protocol paths
	// need the registry, and doing it once before the first frame keeps the
	// dispatcher simple.
	bundle, err := materialiseBundle(provider)
	if err != nil {
		logger.Error("Bundle registration failed", "error", err)
		return fmt.Errorf("registering dags: %w", err)
	}

	comm := NewCoordinatorComm(commConn, commConn, logger)

	frame, err := comm.ReadMessage()
	if err != nil {
		logger.Error("Failed to read initial message from supervisor", "error", err)
		return fmt.Errorf("reading initial message: %w", err)
	}

	if apiErr := apiErrorFromFrame(frame); apiErr != nil {
		logger.Error("Supervisor reported an error on the initial frame",
			"error", apiErr.Err,
			"detail", apiErr.Detail,
		)
		return fmt.Errorf("received error from supervisor: %w", apiErr)
	}

	body, err := decodeIncomingBody(frame.Body)
	if err != nil {
		logger.Error("Failed to decode initial message", "error", err)
		return fmt.Errorf("decoding initial message: %w", err)
	}

	switch msg := body.(type) {
	case *genmodels.StartupDetails:
		logger.Debug("Task execution mode",
			"dag_id", msg.TI.DagID,
			"task_id", msg.TI.TaskID,
		)
		result := RunTask(ctx, bundle, msg, comm, logger)
		// Bound the terminal write so a wedged socket cannot hang shutdown.
		_ = commConn.SetWriteDeadline(time.Now().Add(terminalSendTimeout))
		if err := comm.SendRequest(frame.ID, result); err != nil {
			return fmt.Errorf("sending task result: %w", err)
		}
		logger.Debug("Task execution complete")

	default:
		logger.Error("Unexpected initial message type", "type", fmt.Sprintf("%T", body))
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
