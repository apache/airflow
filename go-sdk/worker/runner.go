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
	"time"

	"github.com/apache/airflow/go-sdk/pkg/api"
	apiContext "github.com/apache/airflow/go-sdk/pkg/context"
	"github.com/apache/airflow/go-sdk/pkg/logging"
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

		RunForever(ctx context.Context, server string) error
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

func (w *worker) ExecuteTaskActivity(ctx context.Context, activity api.ExecuteTaskActivity) {
	// Store the activity in the context so we can get at task id, etc, variables
	taskContext, cancelTaskCtx := context.WithCancel(context.WithValue(ctx, apiContext.ActivityContextKey, activity))

	taskLogger, err := w.setupTaskLogger(ctx, activity)
	if err != nil {
		w.logger.ErrorContext(taskContext, "Could not create logger", slog.Any("error", err))
		return
	}

	task, exists := w.LookupTask(activity.TI.DagId, activity.TI.TaskId)
	if !exists {
		taskLogger.ErrorContext(taskContext, "Task not registered", "dag_id", activity.TI.DagId, "task_id", activity.TI.TaskId)
		return
	}
	taskLogger.InfoContext(taskContext, "Task starting")

	activityClient, err := w.client.WithBearerToken(activity.Token)
	if err != nil {
		w.logger.ErrorContext(taskContext, "Could not create client", slog.Any("error", err))
		return
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
		taskLogger.ErrorContext(taskContext, "Error reporting task as started", slog.Any("error", err))
		return
	} else if resp.StatusCode >= 400 {
		if resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		taskLogger.ErrorContext(taskContext, "Server reported task as started", slog.Any("error", err))
		return

	}

	stopHeartbeating := make(chan bool)
	defer func() {
		if r := recover(); r != nil {
			// Create a new context that isn't cancelled to give us time to report
			ctx, cancel := context.WithTimeout(context.WithoutCancel(taskContext), ReportTimeout)
			defer cancel()
			stopHeartbeating <- true
			taskLogger.ErrorContext(ctx, "Recovered in f", slog.Any("error", r))
			payload, err := json.Marshal(api.TITerminalStatePayload{
				State:   api.TerminalStateNonSuccess(api.TerminalTIStateFailed),
				EndDate: time.Now().UTC(),
			})
			if err != nil {
				taskLogger.ErrorContext(ctx, "Unable to error to server", slog.Any("error", err))
				return
			}
			bodyReader := bytes.NewReader(payload)
			resp, err := activityClient.TiUpdateStateWithBody(ctx, activity.TI.Id, "application/json", bodyReader)
			if resp != nil && resp.Body != nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}()

	ticker := time.NewTicker(HeartbeatInterval)
	go func() {
		// Task Logger is for the task's _own_ logs. We want a logger for our heartbeat etc.
		logger := w.logger.With(slog.String("dag_id", activity.TI.DagId), slog.String("task_id", activity.TI.TaskId))
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
				logger.InfoContext(ctx, "heartbeating", slog.Any("error", err))

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
						"status_code", resp.StatusCode,
						// TODO: include the response body here
					)

					// TODO: Put a timeout on waiting for the task to actually cancel?
					cancelTaskCtx()
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
		taskLogger.InfoContext(taskContext, "Task failed with error, marking task as failed", "error", err)
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

	if err != nil {
		taskLogger.ErrorContext(ctx, "Unable to error to server", slog.Any("error", err))
		return
	}
	bodyReader := bytes.NewReader(finalStatePayload)
	resp, err = activityClient.TiUpdateStateWithBody(ctx, activity.TI.Id, "application/json", bodyReader)
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	if err != nil {
		taskLogger.Error("Error reporting success", "error", err, "final_state", finalState)
	} else if resp.StatusCode >= 400 {
		taskLogger.Error("Error reporting success", "error", err, "final_state", finalState, "status_code", resp.StatusCode)
	}
}

func (w *worker) setupTaskLogger(ctx context.Context, activity api.ExecuteTaskActivity) (*slog.Logger, error) {
	// Create a logger that:
	// - only exits the go-routine on panic, not the whole program
	// - Writes JSON to a file
	// - And streams output to stdout in a nice format too

	// TODO: log to the file name specified in `activity`
	taskLogger := slog.New(
		logging.NewTeeLogger(
			slog.NewJSONHandler(
				os.Stdout,
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

func (w *worker) RunForever(ctx context.Context, server string) error {
	var err error
	w.client, err = api.NewDefaultClient(server)
	if err != nil {
		return err
	}

	w.logger.Info("Starting up")

	ch := make(chan api.ExecuteTaskActivity)
	// ch := w.client.PollQueue(ctx)

	go func() {
		ch <- api.ExecuteTaskActivity{}
	}()

	for true {
		select {
		case <-ctx.Done():
			return nil
		case activity, ok := <-ch:
			if ok {
				w.logger.Debug("Got activity", slog.Any("activity", activity))
				w.ExecuteTaskActivity(ctx, activity)
				w.logger.Debug("activity complete")
			} else {
				w.logger.Info("poll closed")
				break
			}
		}
	}

	return ctx.Err()
}
