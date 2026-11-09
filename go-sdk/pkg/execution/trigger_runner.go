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
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

// This file runs a task from airflow.TriggerDagRun the way TriggerDagRunOperator.execute and
// execute_complete in the standard provider do, with the difference that nothing is Jinja-rendered.

const (
	dagStateTriggerClasspath = "airflow.providers.standard.triggers.external_task.DagStateTrigger"
	// triggerDagRunLinkXComKey is the XCom that the "Triggered DAG" extra link reads.
	triggerDagRunLinkXComKey = "_link_TriggerDagRunLink"
	triggerRunIDXComKey      = "trigger_run_id"
	// triggerFailMethod is the next_method of a task whose trigger failed or timed out.
	triggerFailMethod     = "__fail__"
	executeCompleteMethod = "execute_complete"

	defaultTriggerPokeInterval = 60 * time.Second
)

// The runtime cannot read airflow.cfg, so the coordinator exports these settings.
const (
	apiBaseURLEnv             = "AIRFLOW__API__BASE_URL"
	defaultDeferrableEnv      = "AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE"
	triggererQueuesEnabledEnv = "AIRFLOW__TRIGGERER__QUEUES_ENABLED"
)

const runIDSuffixChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// triggerEnv holds what a trigger task takes from its surroundings, so a test can replace it.
type triggerEnv struct {
	now          func() time.Time
	getenv       func(string) string
	sleep        func(ctx context.Context, d time.Duration) error
	randomSuffix func() string
}

var defaultTriggerEnv = triggerEnv{
	now:    time.Now,
	getenv: os.Getenv,
	sleep: func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	},
	randomSuffix: func() string {
		out := make([]byte, 8)
		for i := range out {
			n, err := rand.Int(rand.Reader, big.NewInt(int64(len(runIDSuffixChars))))
			if err != nil {
				panic(err)
			}
			out[i] = runIDSuffixChars[n.Int64()]
		}
		return string(out)
	},
}

// runTriggerDagRun runs a trigger task and returns the terminal body for the supervisor. It
// returns a genmodels.DeferTask for a task that waits and is deferrable.
func runTriggerDagRun(
	ctx context.Context,
	details *genmodels.StartupDetails,
	spec bundle.TriggerSpec,
	client *CoordinatorClient,
	logger *slog.Logger,
	env triggerEnv,
) (result any) {
	shouldRetry := details.TIContext.ShouldRetry
	fail := func(message string) any {
		logger.ErrorContext(ctx, "Task failed", "error", message)
		return failTask(shouldRetry, errors.New(message))
	}
	defer func() {
		if r := recover(); r != nil {
			logger.ErrorContext(ctx, "Recovered panic in task",
				"error", r, "stack", string(debug.Stack()),
			)
			result = failTask(shouldRetry, fmt.Errorf("panic: %v", r))
		}
	}()
	allowed := spec.AllowedStates
	if len(allowed) == 0 {
		allowed = []string{string(genmodels.DagRunStateSuccess)}
	}
	failed := spec.FailedStates
	if failed == nil {
		failed = []string{string(genmodels.DagRunStateFailed)}
	}

	if nextMethod := ifaceString(details.TIContext.NextMethod); nextMethod != "" {
		return resumeTriggerDagRun(
			nextMethod, details.TIContext.NextKwargs, spec.DagID, allowed, failed, logger, fail,
		)
	}

	logicalDate := triggerLogicalDate(spec, env.now().UTC())
	runID := spec.RunID
	if runID == "" {
		// Python names the run after run_after, which is the logical date when the task sets none.
		runAfter := logicalDate
		if !spec.RunAfter.IsZero() {
			runAfter = spec.RunAfter
		}
		runID = "manual__" + pythonIsoformat(runAfter)
		if logicalDate.IsZero() {
			runID += "_" + env.randomSuffix()
		}
	}

	if spec.FailWhenDagIsPaused {
		paused, err := client.isDagPaused(ctx, spec.DagID)
		if err != nil {
			return fail(err.Error())
		}
		if paused {
			return fail(fmt.Sprintf("Dag %s is paused", spec.DagID))
		}
	}

	ti := taskInstanceOf(details)
	link := dagRunURL(env.getenv(apiBaseURLEnv), spec.DagID, runID)
	if err := client.PushXCom(ctx, ti, triggerDagRunLinkXComKey, link); err != nil {
		return fail(err.Error())
	}

	logger.InfoContext(ctx, "Triggering Dag Run.", "trigger_dag_id", spec.DagID)
	msg := genmodels.TriggerDagRun{
		DagID:       spec.DagID,
		RunID:       runID,
		ResetDagRun: spec.ResetDagRun,
	}
	if !logicalDate.IsZero() {
		msg.LogicalDate = logicalDate
	}
	if !spec.RunAfter.IsZero() {
		msg.RunAfter = spec.RunAfter.UTC()
	}
	if spec.Conf != nil {
		conf := genmodels.Conf(spec.Conf)
		msg.Conf = &conf
	}
	if spec.Note != "" {
		msg.Note = spec.Note
	}
	alreadyExists, err := client.triggerDagRun(ctx, msg)
	if err != nil {
		return fail(err.Error())
	}
	if alreadyExists {
		if spec.SkipWhenAlreadyExists {
			logger.InfoContext(ctx,
				"Dag Run already exists, skipping task as skip_when_already_exists is set to True.",
				"dag_id", spec.DagID,
			)
			return genmodels.TaskState{
				State:   genmodels.TaskStateStateSkipped,
				EndDate: time.Now().UTC(),
			}
		}
		logger.ErrorContext(ctx, "Dag Run already exists, marking task as failed.",
			"dag_id", spec.DagID,
		)
		return genmodels.TaskState{State: genmodels.TaskStateStateFailed, EndDate: time.Now().UTC()}
	}
	logger.InfoContext(ctx, "Dag Run triggered successfully.", "trigger_dag_id", spec.DagID)
	if err := client.PushXCom(ctx, ti, triggerRunIDXComKey, runID); err != nil {
		return fail(err.Error())
	}

	deferrable, err := triggerIsDeferrable(spec, env)
	if err != nil {
		return fail(err.Error())
	}
	if !spec.WaitForCompletion {
		if deferrable {
			logger.InfoContext(ctx,
				"Ignoring deferrable=True because wait_for_completion=False. "+
					"Task will complete immediately without waiting for the triggered DAG run.",
				"trigger_dag_id", spec.DagID,
			)
		}
		return succeedTask()
	}

	pokeInterval := defaultTriggerPokeInterval
	if spec.PokeInterval != nil {
		pokeInterval = *spec.PokeInterval
	}
	if deferrable {
		logger.InfoContext(ctx, "Pausing task as DEFERRED.",
			"trigger_dag_id", spec.DagID, "run_id", runID,
		)
		queuesEnabled, err := envBool(env.getenv, triggererQueuesEnabledEnv)
		if err != nil {
			return fail(err.Error())
		}
		deferred := genmodels.DeferTask{
			State:     "deferred",
			Classpath: dagStateTriggerClasspath,
			// The keys are those of DagStateTrigger.serialize().
			TriggerKwargs: &genmodels.TriggerKwargs{
				"dag_id":          spec.DagID,
				"states":          slices.Concat(allowed, failed),
				"poll_interval":   int(pokeInterval / time.Second),
				"run_ids":         []string{runID},
				"execution_dates": nil,
			},
			NextMethod: executeCompleteMethod,
			NextKwargs: &genmodels.NextKwargs{},
		}
		// The trigger runs on a triggerer that serves the queue of the task only when
		// [triggerer] queues_enabled is set.
		if queuesEnabled && details.TI.Queue != "" {
			deferred.Queue = details.TI.Queue
		}
		return deferred
	}

	for {
		logger.InfoContext(ctx, "Waiting for dag run to complete execution in allowed state.",
			"dag_id", spec.DagID, "run_id", runID, "allowed_state", allowed,
		)
		if err := env.sleep(ctx, pokeInterval); err != nil {
			return fail(err.Error())
		}
		state, err := client.getDagRunState(ctx, spec.DagID, runID)
		if err != nil {
			return fail(err.Error())
		}
		if slices.Contains(failed, state) {
			logger.ErrorContext(ctx, "DagRun finished with failed state.",
				"dag_id", spec.DagID, "state", state,
			)
			return fail(fmt.Sprintf("%s failed with failed state %s", spec.DagID, state))
		}
		if slices.Contains(allowed, state) {
			logger.InfoContext(ctx, "DagRun finished with allowed state.",
				"dag_id", spec.DagID, "state", state,
			)
			return succeedTask()
		}
		logger.DebugContext(ctx, "DagRun not yet in allowed or failed state.",
			"dag_id", spec.DagID, "state", state,
		)
	}
}

// triggerLogicalDate returns the logical date of the Dag run to trigger, or the zero Time for a Dag
// run with none, as TriggerDagRunOperator.execute picks it. It is now only when the task sets
// neither LogicalDate nor RunAfter.
func triggerLogicalDate(spec bundle.TriggerSpec, now time.Time) time.Time {
	switch {
	case !spec.LogicalDate.IsZero():
		return spec.LogicalDate.UTC()
	case spec.RunAfter.IsZero():
		return now
	}
	return time.Time{}
}

// triggerIsDeferrable returns whether the task defers, which is spec.Deferrable or else
// [operators] default_deferrable.
func triggerIsDeferrable(spec bundle.TriggerSpec, env triggerEnv) (bool, error) {
	if spec.Deferrable != nil {
		return *spec.Deferrable, nil
	}
	return envBool(env.getenv, defaultDeferrableEnv)
}

// envBool reads a setting that the coordinator exports as True or False. An unset setting is
// false, which is Python's fallback for the settings of a trigger task.
func envBool(getenv func(string) string, name string) (bool, error) {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("%s is %q, which is not a boolean; use True or False", name, raw)
	}
	return value, nil
}

// resumeTriggerDagRun finishes a task that came back from a deferral, as BaseOperator.resume_execution
// does: __fail__ fails the task, and execute_complete checks the states of the Dag runs.
func resumeTriggerDagRun(
	nextMethod string,
	nextKwargs *genmodels.NextKwargs,
	dagID string,
	allowed, failed []string,
	logger *slog.Logger,
	fail func(string) any,
) any {
	var kwargs genmodels.NextKwargs
	if nextKwargs != nil {
		kwargs = *nextKwargs
	}
	switch nextMethod {
	case triggerFailMethod:
		if traceback, ok := kwargs["traceback"].([]any); ok {
			lines := make([]string, len(traceback))
			for i, line := range traceback {
				lines[i] = fmt.Sprint(line)
			}
			logger.Error("Trigger failed:\n" + strings.Join(lines, "\n"))
		}
		message := "Unknown"
		if value, ok := kwargs["error"]; ok && value != nil {
			message = fmt.Sprint(value)
		}
		return fail(message)
	case executeCompleteMethod:
	default:
		return fail(fmt.Sprintf("Task cannot resume with next_method %q", nextMethod))
	}

	event, ok := decodeTriggerEvent(kwargs["event"])
	runIDs, hasRunIDs := event["run_ids"].([]any)
	if !ok || !hasRunIDs {
		encoded, _ := json.Marshal(kwargs["event"])
		return fail("Task resumed with an event it cannot read: " + string(encoded))
	}
	var failedRunIDs []string
	for _, id := range runIDs {
		runID := fmt.Sprint(id)
		state, _ := event[runID].(string)
		if slices.Contains(failed, state) {
			failedRunIDs = append(failedRunIDs, runID)
		} else if slices.Contains(allowed, state) {
			logger.Info("Triggered Dag run finished with allowed state.",
				"dag_id", dagID, "state", state, "run_id", runID,
			)
		}
	}
	if len(failedRunIDs) > 0 {
		states, _ := json.Marshal(failed)
		runs, _ := json.Marshal(failedRunIDs)
		return fail(fmt.Sprintf(
			"%s failed with failed states %s for run_ids %s", dagID, states, runs,
		))
	}
	return succeedTask()
}

// decodeTriggerEvent returns the data of the event that DagStateTrigger fired. The event is the
// pair (classpath, data). The triggerer stores it with serde, which writes a tuple as
// {"__classname__": "builtins.tuple", "__data__": [...]}.
func decodeTriggerEvent(event any) (map[string]any, bool) {
	if wrapped, ok := event.(map[string]any); ok && wrapped["__classname__"] == "builtins.tuple" {
		event = wrapped["__data__"]
	}
	pair, ok := event.([]any)
	if !ok || len(pair) != 2 {
		return nil, false
	}
	data, ok := pair[1].(map[string]any)
	return data, ok
}

// pythonIsoformat returns t in UTC as Python's datetime.isoformat() writes it, such as
// "2026-09-30T01:02:03.500000+00:00". It writes the microseconds, six digits, only when they are
// not zero.
func pythonIsoformat(t time.Time) string {
	t = t.UTC()
	layout := "2006-01-02T15:04:05"
	if t.Nanosecond()/1000 != 0 {
		layout += ".000000"
	}
	return t.Format(layout) + "+00:00"
}

// dagRunURL returns the URL of a Dag run in the UI, as build_airflow_dagrun_url does on
// [api] base_url. An unset base_url is "/".
func dagRunURL(baseURL, dagID, runID string) string {
	if baseURL == "" {
		baseURL = "/"
	}
	return strings.TrimRight(baseURL, "/") + "/dags/" + dagID + "/runs/" + runID
}
