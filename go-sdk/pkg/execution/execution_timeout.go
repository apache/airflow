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

// executionTimeout is a task's execution_timeout. Call start before run.
type executionTimeout interface {
	start(ctx context.Context, client *CoordinatorClient) error
	run(
		stop context.Context,
		shouldRetry bool,
		logger *slog.Logger,
		run func(ctx context.Context) any,
	) any
}

// resolveExecutionTimeout reads ti_context.execution_timeout. As in Python, zero means no timeout
// and a negative value has already elapsed.
func resolveExecutionTimeout(fromContext any) (executionTimeout, error) {
	if fromContext == nil {
		return disabledExecutionTimeout{}, nil
	}
	seconds, ok := ifaceFloat(fromContext)
	switch {
	case !ok:
		return nil, fmt.Errorf(
			"ti_context.execution_timeout has unexpected wire shape %T",
			fromContext,
		)
	case seconds == 0:
		return disabledExecutionTimeout{}, nil
	case seconds < 0:
		return nil, fmt.Errorf("execution_timeout of %g seconds has already elapsed", seconds)
	}
	return enabledExecutionTimeout{seconds: seconds}, nil
}

type disabledExecutionTimeout struct{}

func (disabledExecutionTimeout) start(context.Context, *CoordinatorClient) error {
	return nil
}

func (disabledExecutionTimeout) run(
	stop context.Context,
	_ bool,
	_ *slog.Logger,
	run func(ctx context.Context) any,
) any {
	return run(stop)
}

type enabledExecutionTimeout struct {
	seconds float64
}

// start arms the supervisor, which sends SIGTERM killed_task_cleanup_time after the timeout.
func (t enabledExecutionTimeout) start(ctx context.Context, client *CoordinatorClient) error {
	_, err := client.comm.Communicate(ctx, genmodels.SetExecutionTimeout{TimeoutSeconds: t.seconds})
	return err
}

// run cancels the task's ctx with sdk.ErrTaskTimeout at the timeout, then waits until the task
// returns or the supervisor's SIGTERM cancels stop. The task fails with the timeout either way.
func (t enabledExecutionTimeout) run(
	stop context.Context,
	shouldRetry bool,
	logger *slog.Logger,
	run func(ctx context.Context) any,
) any {
	timeoutErr := fmt.Errorf("%w after %g seconds", sdk.ErrTaskTimeout, t.seconds)
	ctx, cancel := context.WithTimeoutCause(stop, secondsToDuration(t.seconds), timeoutErr)
	defer cancel()

	done := make(chan taskOutcome, 1)
	go func() {
		result := run(ctx)
		// Whichever cancels ctx first wins, so after this the timer cannot change the cause.
		cancel()
		done <- taskOutcome{result, errors.Is(context.Cause(ctx), sdk.ErrTaskTimeout)}
	}()

	select {
	case outcome := <-done:
		if !outcome.timedOut {
			return outcome.result
		}
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), sdk.ErrTaskTimeout) {
			// A SIGTERM before the timeout, or the task has just returned: wait for its result.
			return (<-done).result
		}
		logger.Warn("Task reached its execution_timeout; waiting for it to stop",
			"timeout_seconds", t.seconds,
		)
		select {
		case <-done:
		case <-stop.Done():
		}
	}
	logger.Error("Task timed out", "error", timeoutErr)
	return failTask(shouldRetry, timeoutErr)
}

type taskOutcome struct {
	result   any
	timedOut bool
}

// maxDurationSeconds is the longest timeout time.Duration can hold, about 292 years.
const maxDurationSeconds = float64(math.MaxInt64) / float64(time.Second)

func secondsToDuration(seconds float64) time.Duration {
	if seconds >= maxDurationSeconds {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(seconds * float64(time.Second))
}
