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
	"io"
	"log/slog"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"time"

	"github.com/spf13/viper"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/logging"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

type (
	// task is an interface of an task implementation.
	Task interface {
		Execute(ctx context.Context, logger *slog.Logger) error
	}
	// Bundle interface defines a type that is used "at execution time" to lookup a Task to execute
	Bundle interface {
		LookupTask(dagId, taskId string) (Task, bool)
	}

	Worker interface {
		Bundle

		ExecuteTaskWorkload(ctx context.Context, workload api.ExecuteTaskWorkload) error

		WithServer(server string) (Worker, error)
		WithClient(client api.ClientInterface) Worker
		WithHeartbeatInterval(interval time.Duration) Worker
	}

	worker struct {
		Bundle
		client            api.ClientInterface
		logger            *slog.Logger
		heartbeatInterval time.Duration
		reportTimeout     time.Duration
	}
)

var ErrTaskCancelledAfterFailedHeartbeat = errors.New("task cancelled after failed heartbeat")

func NewWithBundle(bundle Bundle, logger *slog.Logger) Worker {
	return &worker{
		logger:            logger,
		heartbeatInterval: HeartbeatInterval,
		reportTimeout:     ReportTimeout,
		Bundle:            bundle,
	}
}

func New(logger *slog.Logger) Worker {
	return &worker{
		logger:            logger,
		heartbeatInterval: HeartbeatInterval,
		reportTimeout:     ReportTimeout,
	}
}

const (
	HeartbeatInterval = 5 * time.Second
	ReportTimeout     = 10 * time.Second
)

func (w *worker) WithHeartbeatInterval(interval time.Duration) Worker {
	newWorker := *w
	newWorker.heartbeatInterval = interval
	return &newWorker
}

func (w *worker) WithClient(client api.ClientInterface) Worker {
	newWorker := *w
	newWorker.client = client
	return &newWorker
}

func (w *worker) WithServer(server string) (Worker, error) {
	var err error
	newWorker := *w
	newWorker.client, err = api.NewDefaultClient(server)
	if err != nil {
		return nil, err
	}
	return &newWorker, nil
}

type heartbeater struct {
	heartbeatInterval time.Duration
	logger            *slog.Logger
	taskCanceller     context.CancelCauseFunc
	taskLogger        *slog.Logger
}

// Run a heartbeat loop until our ctx is Done
//
// If there is an error reporting too many heartbeats then the task context will be cancelled to stop the task.
func (h *heartbeater) Run(
	ctx context.Context,
) {
	h.logger.DebugContext(ctx, "Starting heartbeater", "heartbeat", h.heartbeatInterval)

	workload := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
	client := ctx.Value(sdkcontext.ApiClientContextKey).(api.ClientInterface)

	ticker := time.NewTicker(h.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// TODO: Record when last successful heartbeat was, and fail+abort if we haven't managed to record
			// one recently enough

			err := client.TaskInstances().
				Heartbeat(ctx, workload.TI.Id, &api.TIHeartbeatInfo{
					Hostname: Hostname,
					Pid:      os.Getpid(),
				})
			if err != nil {
				h.logger.InfoContext(ctx, "heartbeating", slog.Any("error", err))
			}

			var httpError *api.GeneralHTTPError
			if errors.As(err, &httpError) {
				resp := httpError.Response
				if resp != nil && resp.StatusCode() == 404 || resp.StatusCode() == 409 {
					h.logger.ErrorContext(
						ctx,
						"Server indicated the task shouldn't be running anymore",
						"status_code", resp.StatusCode(),
						"details", httpError.JSON,
					)
					// Log something in the task log file too
					h.taskLogger.ErrorContext(
						ctx,
						"Server indicated the task shouldn't be running anymore. Terminating workload",
						"status_code",
						resp.StatusCode(),
						"details",
						httpError.JSON,
					)

					h.taskCanceller(ErrTaskCancelledAfterFailedHeartbeat)
					return
				}

			}
		case <-ctx.Done():
			// Something signaled us to stop.
			return
		}
	}
}

func (w *worker) ExecuteTaskWorkload(ctx context.Context, workload api.ExecuteTaskWorkload) error {
	if hostname := viper.GetString("hostname"); hostname != "" {
		Hostname = hostname
	}

	// Store the workload in the context so we can get at task id, etc, variables
	taskContext, cancelTaskCtx := context.WithCancelCause(
		context.WithValue(ctx, sdkcontext.WorkloadContextKey, workload),
	)
	defer cancelTaskCtx(context.Canceled)

	// Task Logger is for the task's _own_ logs. We want a logger for our heartbeat etc.
	logger := w.logger.With(
		slog.String("dag_id", workload.TI.DagId),
		slog.String("task_id", workload.TI.TaskId),
		slog.String("ti_id", workload.TI.Id.String()),
	)

	workloadClient := w.client
	if c, ok := workloadClient.(*api.Client); ok {
		var err error
		workloadClient, err = c.WithBearerToken(workload.Token)
		if err != nil {
			logger.ErrorContext(ctx, "Could not create client", slog.Any("error", err))
			return err
		}

		c = workloadClient.(*api.Client)
		c.Client.SetLogger(&logging.RestyLoggerBridge{Handler: logger.Handler(), Context: ctx})
		c.Client.SetDebug(viper.GetBool("api_client.debug"))
	}

	reportStateFailed := func(ctx context.Context) error {
		body := &api.TIUpdateStatePayload{}
		body.FromTITerminalStatePayload(api.TITerminalStatePayload{
			State:   api.TerminalStateNonSuccess(api.TerminalTIStateFailed),
			EndDate: time.Now().UTC(),
		})
		return workloadClient.TaskInstances().UpdateState(
			ctx,
			workload.TI.Id,
			body,
		)
	}

	// Store the configured API client in the context so we can get it out for accessing Variables etc.
	taskContext = context.WithValue(taskContext, sdkcontext.ApiClientContextKey, workloadClient)

	taskLogger, err := w.setupTaskLogger(ctx, logger, workload)
	if err != nil {
		logger.ErrorContext(taskContext, "Could not create logger", slog.Any("error", err))
		_ = reportStateFailed(ctx)
		return err
	}

	var task Task
	var exists bool = w.Bundle != nil
	if exists {
		// Only try looking up the task if we actually have a bundle
		task, exists = w.LookupTask(workload.TI.DagId, workload.TI.TaskId)
	}
	if !exists {
		taskLogger.ErrorContext(
			taskContext,
			"Task not registered",
			"dag_id",
			workload.TI.DagId,
			"task_id",
			workload.TI.TaskId,
		)
		return reportStateFailed(ctx)
	}

	// TODO: Timeout etc on the context
	// TODO: Add in retries on the api client

	runtimeContext, err := workloadClient.TaskInstances().
		Run(ctx, workload.TI.Id, &api.TIEnterRunningPayload{
			Hostname:  Hostname,
			Unixname:  Username,
			Pid:       PID,
			State:     api.Running,
			StartDate: time.Now().UTC(),
		})
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

	// Make the heartbreat Context a child of the task context, so it finishes automatically when the task
	// context does.
	heartbeatCtx, stopHeartbeating := context.WithCancel(taskContext)
	defer stopHeartbeating()

	defer func() {
		if r := recover(); r != nil {
			stopHeartbeating()

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
			_ = reportStateFailed(ctx)
		}
	}()

	heartbeater := &heartbeater{
		heartbeatInterval: w.heartbeatInterval,
		logger:            logger,
		taskCanceller:     cancelTaskCtx,
		taskLogger:        taskLogger,
	}

	go heartbeater.Run(heartbeatCtx)

	err = task.Execute(taskContext, taskLogger)
	endTime := time.Now().UTC()

	stopHeartbeating()

	var finalState api.TerminalTIState
	body := &api.TIUpdateStatePayload{}

	if taskContext.Err() == ErrTaskCancelledAfterFailedHeartbeat {
		// We've already logged when we failed to heartbeat, don't do it again
		finalState = api.TerminalTIStateFailed
		body.FromTITerminalStatePayload(api.TITerminalStatePayload{
			State:   api.TerminalStateNonSuccess(finalState),
			EndDate: time.Now().UTC(),
		})
	} else if err != nil {
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
		if logger != taskLogger {
			// If we are sending task logs to stdout, don't log this here else we'll get it twice
			logger.InfoContext(ctx, "Task succeeded")
		}

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
	err = workloadClient.TaskInstances().UpdateState(
		ctx,
		workload.TI.Id,
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

type taskJSONHandler struct {
	*slog.JSONHandler
}

var _ slog.Handler = (*taskJSONHandler)(nil)

// Make the keys and predefined values (log level) in the same form that Airflow expects.
func jsonAttrKeyReplacer(groups []string, a slog.Attr) slog.Attr {
	if len(groups) == 0 {
		if a.Key == slog.MessageKey {
			a.Key = "event"
		}
		if a.Key == slog.LevelKey {
			a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
		}
	}
	// TODO: mask secrets here
	return a
}

func newTaskJSONHandler(w io.Writer) slog.Handler {
	return &taskJSONHandler{slog.NewJSONHandler(
		w,
		&slog.HandlerOptions{
			AddSource:   false,
			ReplaceAttr: jsonAttrKeyReplacer,
		},
	)}
}

func (t *taskJSONHandler) Handle(ctx context.Context, r slog.Record) error {
	// We want to change the "time" key to "timestamp". While we could achieve that with a ReplaceAttr hook in
	// the opts, we can only do that based on attry name, so if something else tries to log an attr of "time",
	// that would get replaced too. So we do it the slower way, by wrapping this handler
	//
	// TODO: This isn't perfect, as it places "timestamp" after "level" etc. But it'll do for now

	r2 := slog.NewRecord(time.Time{}, r.Level, r.Message, r.PC)
	if !r.Time.IsZero() {
		r2.AddAttrs(slog.String("timestamp", r.Time.Format(time.RFC3339Nano)))
	}
	r.Attrs(func(a slog.Attr) bool {
		r2.AddAttrs(a)
		return true
	})
	return t.JSONHandler.Handle(ctx, r2)
}

func (w *worker) setupTaskLogger(
	_ context.Context,
	supervisorLogger *slog.Logger,
	workload api.ExecuteTaskWorkload,
) (*slog.Logger, error) {
	// Create a logger that:
	// - only exits the go-routine on panic, not the whole program
	// - Writes JSON to a file
	// - And streams output to stdout in a nice format too

	if viper.GetBool("logging.task.stdout_only") || workload.LogPath == nil {
		return supervisorLogger, nil
	}

	base := viper.GetString("logging.base_log_folder")
	filename := path.Join(base, *workload.LogPath)
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

	var h slog.Handler = newTaskJSONHandler(fh)

	if viper.GetBool("logging.task.to_stdout") {
		h = logging.NewTeeLogger(h, supervisorLogger.Handler())
	}

	taskLogger := slog.New(h)
	return taskLogger, nil
}
