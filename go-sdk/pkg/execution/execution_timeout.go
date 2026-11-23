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
	"log/slog"
	"math"
	"time"

	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
	"github.com/apache/airflow/go-sdk/sdk"
)

// resolveExecutionTimeout returns the task's execution_timeout in seconds from ti_context, or nil
// when the task has no timeout. The coordinator has already filled in the worker's default.
// As in the Python task runner, zero means no timeout and a negative value has already elapsed.
func resolveExecutionTimeout(fromContext any) (*float64, error) {
	if fromContext == nil {
		return nil, nil
	}
	seconds, ok := ifaceFloat(fromContext)
	switch {
	case !ok:
		return nil, fmt.Errorf(
			"ti_context.execution_timeout has unexpected wire shape %T",
			fromContext,
		)
	case seconds == 0:
		return nil, nil
	case seconds < 0:
		return nil, fmt.Errorf("execution_timeout of %g seconds has already elapsed", seconds)
	}
	return &seconds, nil
}

// setExecutionTimeout tells the supervisor the task's execution_timeout and restarts its clock from
// when the supervisor receives it. The supervisor sends SIGTERM killed_task_cleanup_time after the
// timeout, which ends the cleanup time runWithExecutionTimeout gives the task.
func (c *CoordinatorClient) setExecutionTimeout(ctx context.Context, seconds float64) error {
	_, err := c.comm.Communicate(ctx, genmodels.SetExecutionTimeout{TimeoutSeconds: seconds})
	return err
}

// runWithExecutionTimeout runs the task and returns its terminal body, failing the task when it
// runs past the timeout. Call it after setExecutionTimeout returns, so that its clock starts after
// the supervisor's, as in the Python task runner.
//
// At the timeout, the task's ctx is cancelled with sdk.ErrTaskTimeout as its cause. The task then
// has until it returns, or until the supervisor's SIGTERM cancels stop, to clean up. Either way the
// task fails with the timeout, so a task that returns nil after the timeout does not succeed. A
// task that ignores its ctx keeps running in its goroutine until the runtime process exits after
// reporting the timeout.
func runWithExecutionTimeout(
	stop context.Context,
	seconds float64,
	shouldRetry bool,
	logger *slog.Logger,
	run func(ctx context.Context) any,
) any {
	timeoutErr := fmt.Errorf("%w after %g seconds", sdk.ErrTaskTimeout, seconds)
	ctx, cancel := context.WithTimeoutCause(stop, secondsToDuration(seconds), timeoutErr)
	defer cancel()

	done := make(chan any, 1)
	go func() { done <- run(ctx) }()

	select {
	case result := <-done:
		if !errors.Is(context.Cause(ctx), sdk.ErrTaskTimeout) {
			return result
		}
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), sdk.ErrTaskTimeout) {
			// A SIGTERM before the timeout: wait for the task, as without a timeout.
			return <-done
		}
		logger.Warn("Task reached its execution_timeout; waiting for it to stop",
			"timeout_seconds", seconds,
		)
		select {
		case <-done:
		case <-stop.Done():
		}
	}
	logger.Error("Task timed out", "error", timeoutErr)
	return failTask(shouldRetry, timeoutErr)
}

// maxDurationSeconds is the longest timeout time.Duration can hold, about 292 years.
const maxDurationSeconds = float64(math.MaxInt64) / float64(time.Second)

func secondsToDuration(seconds float64) time.Duration {
	if seconds >= maxDurationSeconds {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}
