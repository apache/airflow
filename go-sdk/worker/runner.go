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
	"bytes"
	"context"
	"encoding/json"
	"io"
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
		client *api.Client
		logger *slog.Logger
	}
)

func New(logger *slog.Logger) Worker {
	return &worker{
		logger: logger,
		Registry: &registry{
			taskFuncMap: make(map[string]map[string]Task),
		},
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

func (w *worker) ExecuteTaskActivity(ctx context.Context, activity api.ExecuteTaskActivity) error {
	// Store the activity in the context so we can get at task id, etc, variables
	taskContext, cancelTaskCtx := context.WithCancel(
		context.WithValue(ctx, sdkcontext.ActivityContextKey, activity),
	)
	defer cancelTaskCtx()

	taskLogger, err := w.setupTaskLogger(ctx, activity)
	if err != nil {
		w.logger.ErrorContext(taskContext, "Could not create logger", slog.Any("error", err))
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
		// TODO: We can still report this to the server!
		return nil
	}
	taskLogger.InfoContext(taskContext, "Task starting")

	activityClient, err := w.client.WithBearerToken(activity.Token)
	if err != nil {
		w.logger.ErrorContext(taskContext, "Could not create client", slog.Any("error", err))
		return err
	}

	// TODO: Timeout etc on the context
	// TODO: Add in retries on the api client

	resp, err := activityClient.TiRun(ctx, activity.TI.Id, api.TIEnterRunningPayload{
		Hostname:  Hostname,
		Pid:       os.Getpid(),
		State:     api.Running,
		Unixname:  Username,
		StartDate: time.Now().UTC(),
	})
	if err != nil {
		taskLogger.ErrorContext(
			taskContext,
			"Error reporting task as started",
			slog.Any("error", err),
		)
		return err
	} else if resp.StatusCode >= 400 {
		errResp, err := activityClient.ResponseErrorToJson(resp)
		if err != nil {
			taskLogger.ErrorContext(taskContext, "Error reading error response", slog.Any("error", err))
			return err
		}
		taskLogger.ErrorContext(taskContext, "Server reported error when attempting to start task",
			slog.Any("error", errResp), slog.Int("status_code", resp.StatusCode),
			slog.Group("request", "url", resp.Request.URL, "method", resp.Request.Method, "headers", resp.Request.Header))
		return nil

	}

	stopHeartbeating := make(chan bool)
	defer func() {
		if r := recover(); r != nil {
			// Create a new context that isn't cancelled to give us time to report
			ctx, cancel := context.WithTimeout(context.WithoutCancel(taskContext), ReportTimeout)
			defer cancel()
			stopHeartbeating <- true
			taskLogger.ErrorContext(
				ctx,
				"Recovered in f",
				slog.Any("error", r),
				"stack",
				string(debug.Stack()),
			)
			payload, err := json.Marshal(api.TITerminalStatePayload{
				State:   api.TerminalStateNonSuccess(api.TerminalTIStateFailed),
				EndDate: time.Now().UTC(),
			})
			if err != nil {
				taskLogger.ErrorContext(ctx, "Unable to error to server", slog.Any("error", err))
				return
			}
			bodyReader := bytes.NewReader(payload)
			resp, _ := activityClient.TiUpdateStateWithBody(
				ctx,
				activity.TI.Id,
				"application/json",
				bodyReader,
			)
			if resp != nil && resp.Body != nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}()

	ticker := time.NewTicker(HeartbeatInterval)
	go func() {
		// Task Logger is for the task's _own_ logs. We want a logger for our heartbeat etc.
		logger := w.logger.With(
			slog.String("dag_id", activity.TI.DagId),
			slog.String("task_id", activity.TI.TaskId),
		)
		logger.InfoContext(ctx, "Starting heartbeater")
		for {
			select {
			case <-ticker.C:
				// TODO: Record when last successful heartbeat was, and fail+abort if we haven't managed to record
				// one recently enough

				resp, err := activityClient.TiHeartbeat(ctx, activity.TI.Id, api.TIHeartbeatInfo{
					Hostname: Hostname,
					Pid:      os.Getpid(),
				})
				if err != nil {
					logger.InfoContext(ctx, "heartbeating", slog.Any("error", err))
				}

				if resp != nil && resp.StatusCode == 404 || resp.StatusCode == 409 {
					stopHeartbeating <- true
					logger.ErrorContext(
						ctx,
						"Server indicated the task shouldn't be running anymore",
						"status_code", resp.StatusCode,
						// TODO: include the response body here
					)
					taskLogger.ErrorContext(
						taskContext,
						"Server indicated the task shouldn't be running anymore. Terminating activity",
						"status_code",
						resp.StatusCode,
						// TODO: include the response body here
					)

					// TODO: Put a timeout on waiting for the task to actually cancel?
					return
				}
			case <-taskContext.Done():
				return
			case <-stopHeartbeating:
				return
			}
		}
	}()

	defer ticker.Stop()

	err = task.Execute(taskContext, taskLogger)
	endTime := time.Now().UTC()
	stopHeartbeating <- true

	var finalStatePayload json.RawMessage
	var finalState api.TerminalTIState

	if err != nil {
		taskLogger.InfoContext(
			taskContext,
			"Task failed with error, marking task as failed",
			"error",
			err,
		)
		finalState = api.TerminalTIStateFailed
		finalStatePayload, _ = json.Marshal(api.TITerminalStatePayload{
			EndDate: endTime,
			State:   api.TerminalStateNonSuccess(finalState),
		})
	} else {
		finalState = api.TerminalTIStateSuccess
		finalStatePayload, _ = json.Marshal(api.TISuccessStatePayload{
			EndDate: endTime,
			State:   api.TISuccessStatePayloadState(finalState),
		})
	}

	bodyReader := bytes.NewReader(finalStatePayload)
	resp, err = activityClient.TiUpdateStateWithBody(
		ctx,
		activity.TI.Id,
		"application/json",
		bodyReader,
	)
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	if err != nil {
		taskLogger.Error("Error reporting success", "error", err, "final_state", finalState)
		return err
	} else if resp.StatusCode >= 400 {
		taskLogger.Error("Error reporting success", "error", err, "final_state", finalState, "status_code", resp.StatusCode)
	}

	return err
}

func (w *worker) setupTaskLogger(
	ctx context.Context,
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

	// TODO: log to the file name specified in `activity`
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
