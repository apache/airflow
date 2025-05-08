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

package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path"
	"runtime/debug"
	"time"

	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/logging"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

type (
	Registry interface {
		// RegisterTask registers a new task with this task registry
		// A task takes a [context.Context] and input and returns a (result, error) or just error.
		//
		// This method panics if taskFunc doesn't comply with the expected format or tries to register a duplicate task
		RegisterTask(dagid string, fn any)

		RegisterTaskWithName(dagId, taskId string, fn any)

		LookupTask(dagId, taskId string) (task Task, exists bool)
	}

	Worker interface {
		Registry

		ExecuteTaskActivity(ctx context.Context, activity api.ExecuteTaskActivity) error

		WithServer(server string) (Worker, error)
	}

	worker struct {
		Registry
		client            api.ClientInterface
		logger            *slog.Logger
		heartbeatInterval time.Duration
		reportTimeout     time.Duration
	}
)

func New(logger *slog.Logger) Worker {
	return &worker{
		logger:            logger,
		heartbeatInterval: HeartbeatInterval,
		reportTimeout:     ReportTimeout,
		Registry:          newRegistry(),
	}
}

const (
	HeartbeatInterval = 5 * time.Second
	ReportTimeout     = 10 * time.Second
)

func (w *worker) WithServer(server string) (Worker, error) {
	client, err := api.NewDefaultClient(server)
	if err != nil {
		return nil, err
	}
	return &worker{
		Registry: w.Registry,
		client:   client,
		logger:   w.logger,
	}, nil
}

// heartbeater runs a heartbeat loop until either the taskContext is signaled as done, or a message is
// received on stopChan
func (w *worker) heartbeater(
	ctx context.Context,
	logger *slog.Logger,
	taskContext context.Context,
	taskLogger *slog.Logger,
	stopChan chan bool,
) {
	ticker := time.NewTicker(w.heartbeatInterval)
	defer ticker.Stop()
	logger.InfoContext(ctx, "Starting heartbeater")
	for {
		select {
		case <-ticker.C:
			// TODO: Record when last successful heartbeat was, and fail+abort if we haven't managed to record
			// one recently enough

			activity := taskContext.Value(sdkcontext.ActivityContextKey).(api.ExecuteTaskActivity)
			client := taskContext.Value(sdkcontext.ApiClientContextKey).(api.ClientInterface)
			err := client.TaskInstances().
				Heartbeat(ctx, activity.TI.Id, &api.TIHeartbeatInfo{
					Hostname: Hostname,
					Pid:      os.Getpid(),
				})
			if err != nil {
				logger.InfoContext(ctx, "heartbeating", slog.Any("error", err))
			}

			var httpError *api.GeneralHTTPError
			if errors.As(err, &httpError) {
				resp := httpError.Response
				if resp != nil && resp.StatusCode() == 404 || resp.StatusCode() == 409 {
					stopChan <- true
					logger.ErrorContext(
						ctx,
						"Server indicated the task shouldn't be running anymore",
						"status_code", resp.StatusCode(),
						"details", httpError.JSON,
					)
					taskLogger.ErrorContext(
						taskContext,
						"Server indicated the task shouldn't be running anymore. Terminating activity",
						"status_code",
						resp.StatusCode(),
						"details",
						httpError.JSON,
					)

					// TODO: Put a timeout on waiting for the task to actually cancel?
					return
				}

			}
		case <-taskContext.Done():
			return
		case <-stopChan:
			return
		}
	}
}

func (w *worker) ExecuteTaskActivity(ctx context.Context, activity api.ExecuteTaskActivity) error {
	// Store the activity in the context so we can get at task id, etc, variables
	taskContext, cancelTaskCtx := context.WithCancel(
		context.WithValue(ctx, sdkcontext.ActivityContextKey, activity),
	)
	defer cancelTaskCtx()

	// Task Logger is for the task's _own_ logs. We want a logger for our heartbeat etc.
	logger := w.logger.With(
		slog.String("dag_id", activity.TI.DagId),
		slog.String("task_id", activity.TI.TaskId),
		slog.String("ti_id", activity.TI.Id.String()),
	)

	activityClient := w.client
	if c, ok := activityClient.(*api.Client); ok {
		activityClient, err := c.WithBearerToken(activity.Token)
		if err != nil {
			logger.ErrorContext(ctx, "Could not create client", slog.Any("error", err))
			return err
		}

		c = activityClient.(*api.Client)
		c.SetLogger(&logging.RestyLoggerBridge{Handler: logger.Handler(), Context: ctx})
		if viper.GetBool("api_client.debug") {
			c.EnableDebug()
		}
	}

	reportStateFailed := func() error {
		body := &api.TIUpdateStatePayload{}
		body.FromTITerminalStatePayload(api.TITerminalStatePayload{
			State:   api.TerminalStateNonSuccess(api.TerminalTIStateFailed),
			EndDate: time.Now().UTC(),
		})
		return activityClient.TaskInstances().UpdateState(
			ctx,
			activity.TI.Id,
			body,
		)
	}

	// Store the configured API client in the context so we can get it out for accessing Variables etc.
	taskContext = context.WithValue(taskContext, sdkcontext.ApiClientContextKey, activityClient)

	taskLogger, err := w.setupTaskLogger(ctx, activity)
	if err != nil {
		logger.ErrorContext(taskContext, "Could not create logger", slog.Any("error", err))
		_ = reportStateFailed()
		return err
	}

	task, exists := w.LookupTask(activity.TI.DagId, activity.TI.TaskId)
	if !exists {
		taskLogger.ErrorContext(
			taskContext,
			"Task not registered",
			"dag_id",
			activity.TI.DagId,
			"task_id",
			activity.TI.TaskId,
		)
		return reportStateFailed()
	}

	// TODO: Timeout etc on the context
	// TODO: Add in retries on the api client

	runtimeContext, err := activityClient.TaskInstances().
		Run(ctx, activity.TI.Id, &api.TIEnterRunningPayload{
			Hostname:  Hostname,
			Unixname:  Username,
			Pid:       PID,
			State:     api.Running,
			StartDate: time.Now().UTC(),
		})
	logger.InfoContext(ctx, "Start context", slog.Any("resp", runtimeContext), slog.Any("err", err))
	if err != nil {
		var httpError *api.GeneralHTTPError
		if errors.As(err, &httpError) {
			resp := httpError.Response
			taskLogger.ErrorContext(
				taskContext,
				"Server reported error when attempting to start task",
				slog.Any("error", httpError),
				slog.Int("status_code", resp.StatusCode()),
				slog.Group(
					"request",
					"url",
					resp.Request.URL,
					"method",
					resp.Request.Method,
					"headers",
					resp.Request.Header,
				),
			)
		}
		return err
	}

	taskContext = context.WithValue(taskContext, sdkcontext.RuntimeTIContextKey, runtimeContext)

	stopHeartbeating := make(chan bool)
	defer func() {
		if r := recover(); r != nil {
			stopHeartbeating <- true

			// Create a new context that isn't cancelled to give us time to report
			ctx, cancel := context.WithTimeout(context.WithoutCancel(taskContext), w.reportTimeout)
			defer cancel()
			taskLogger.ErrorContext(
				ctx,
				"Recovered in task",
				slog.Any("error", r),
				"stack",
				string(debug.Stack()),
			)
			reportStateFailed()
		}
	}()

	go w.heartbeater(ctx, logger, taskContext, taskLogger, stopHeartbeating)

	err = task.Execute(taskContext, taskLogger)
	endTime := time.Now().UTC()
	stopHeartbeating <- true

	var finalState api.TerminalTIState
	body := &api.TIUpdateStatePayload{}

	if err != nil {
		logger.InfoContext(ctx, "Task returned an error, execution complete")
		taskLogger.InfoContext(
			taskContext,
			"Task failed with error, marking task as failed",
			"error",
			err,
			"type",
			fmt.Sprintf("%T", err),
		)
		finalState = api.TerminalTIStateFailed
		body.FromTITerminalStatePayload(api.TITerminalStatePayload{
			State:   api.TerminalStateNonSuccess(finalState),
			EndDate: time.Now().UTC(),
		})
	} else {
		logger.InfoContext(ctx, "Task succeeded")

		taskLogger.InfoContext(
			taskContext,
			"Task succeeded",
		)
		finalState = api.TerminalTIStateSuccess
		body.FromTISuccessStatePayload(api.TISuccessStatePayload{
			EndDate: endTime,
			State:   api.TISuccessStatePayloadState(finalState),
		})
	}

	err = activityClient.TaskInstances().UpdateState(
		ctx,
		activity.TI.Id,
		body,
	)
	if err != nil {
		taskLogger.Error(
			"Error reporting final state to server",
			"error",
			err,
			"final_state",
			finalState,
		)
		return err
	}

	return err
}

func (w *worker) setupTaskLogger(
	_ context.Context,
	activity api.ExecuteTaskActivity,
) (*slog.Logger, error) {
	// Create a logger that:
	// - only exits the go-routine on panic, not the whole program
	// - Writes JSON to a file
	// - And streams output to stdout in a nice format too

	base := viper.GetString("logging.base_log_path")
	filename := path.Join(base, *activity.LogPath)
	dir := path.Dir(filename)

	// TODO: umask?
	err := os.MkdirAll(dir, 0o750)
	if err != nil {
		return nil, err
	}

	fh, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	taskLogger := slog.New(
		logging.NewTeeLogger(
			slog.NewJSONHandler(
				fh,
				&slog.HandlerOptions{
					AddSource:   false,
					ReplaceAttr: nil,
				},
			),
			slog.Default().Handler(),
		),
	)
	return taskLogger, nil
}
