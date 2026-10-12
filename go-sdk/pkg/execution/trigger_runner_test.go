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
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/internal/bundle"
	"github.com/apache/airflow/go-sdk/pkg/execution/genmodels"
)

var triggerNow = time.Date(2026, 9, 30, 1, 2, 3, 500000000, time.UTC)

const triggerNowRunID = "manual__2026-09-30T01:02:03.500000+00:00"

// triggerSupervisor answers the requests of a trigger task and keeps them in order.
type triggerSupervisor struct {
	mu       sync.Mutex
	requests []map[string]any

	paused bool
	// triggerErr is the error code that TriggerDagRun answers with, if any.
	triggerErr string
	// states are the answers to GetDagRunState in turn. The last one repeats.
	states []string
}

func (s *triggerSupervisor) answer(req map[string]any) (body, errBody map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
	switch req["type"] {
	case "GetDag":
		return map[string]any{
			"type": "DagResult", "dag_id": req["dag_id"], "is_paused": s.paused,
		}, nil
	case "TriggerDagRun":
		if s.triggerErr != "" {
			return nil, map[string]any{
				"type": "ErrorResponse", "error": s.triggerErr, "detail": map[string]any{},
			}
		}
	case "GetDagRunState":
		state := s.states[0]
		if len(s.states) > 1 {
			s.states = s.states[1:]
		}
		return map[string]any{"type": "DagRunStateResult", "state": state}, nil
	}
	return nil, nil
}

func (s *triggerSupervisor) sent() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), s.requests...)
}

// types returns the type of each request that the task sent, with the key of an XCom.
func (s *triggerSupervisor) types() []string {
	var types []string
	for _, req := range s.sent() {
		kind := req["type"].(string)
		if kind == "SetXCom" {
			kind += ":" + req["key"].(string)
		}
		types = append(types, kind)
	}
	return types
}

func (s *triggerSupervisor) request(t *testing.T, kind string) map[string]any {
	t.Helper()
	for _, req := range s.sent() {
		if req["type"] == kind {
			return req
		}
	}
	require.Failf(t, "request not sent", "the task sent no %s: %v", kind, s.types())
	return nil
}

func (s *triggerSupervisor) comm(t *testing.T) *CoordinatorComm {
	t.Helper()
	reqR, reqW := io.Pipe()
	respR, respW := io.Pipe()
	t.Cleanup(func() {
		reqR.Close()
		reqW.Close()
		respR.Close()
		respW.Close()
	})
	go func() {
		for {
			frame, err := readFrame(reqR)
			if err != nil {
				return
			}
			body, errBody := s.answer(rawToMap(t, frame.Body))
			if err := writeFrame(respW, encodeResponseFrame(t, frame.ID, body, errBody)); err != nil {
				return
			}
		}
	}()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewCoordinatorComm(respR, reqW, logger)
}

// triggerTestEnv replaces the surroundings of a trigger task for the test. It returns the
// durations that the task slept.
func triggerTestEnv(t *testing.T, settings map[string]string) *[]time.Duration {
	t.Helper()
	var slept []time.Duration
	saved := defaultTriggerEnv
	defaultTriggerEnv = triggerEnv{
		now:    func() time.Time { return triggerNow },
		getenv: func(name string) string { return settings[name] },
		sleep: func(ctx context.Context, d time.Duration) error {
			slept = append(slept, d)
			return ctx.Err()
		},
		randomSuffix: func() string { return "AbCd1234" },
	}
	t.Cleanup(func() { defaultTriggerEnv = saved })
	return &slept
}

func runTriggerTask(
	t *testing.T,
	spec bundle.TriggerSpec,
	supervisor *triggerSupervisor,
	adjust func(*genmodels.StartupDetails),
) any {
	t.Helper()
	b := testBundle{"test_dag": testDag{"trigger": &bundle.TriggerTask{Spec: spec}}}
	details := newStartupDetails("trigger")
	details.TI.Queue = "golang"
	if adjust != nil {
		adjust(details)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return RunTask(context.Background(), b, details, supervisor.comm(t), logger)
}

func shouldRetry(details *genmodels.StartupDetails) { details.TIContext.ShouldRetry = true }

func TestTriggerDagRunFireAndForget(t *testing.T) {
	triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{}

	result := runTriggerTask(t, bundle.TriggerSpec{DagID: "downstream"}, supervisor, nil)

	assertSucceedTask(t, result)
	assert.Equal(t,
		[]string{"SetXCom:_link_TriggerDagRunLink", "TriggerDagRun", "SetXCom:trigger_run_id"},
		supervisor.types(),
	)
	sent := supervisor.sent()
	assert.Equal(t, "/dags/downstream/runs/"+triggerNowRunID, sent[0]["value"])
	assert.Equal(t, "run1", sent[0]["run_id"])
	assert.Equal(t, "trigger", sent[0]["task_id"])
	assert.Equal(t, triggerNowRunID, sent[1]["run_id"])
	assert.Equal(t, triggerNowRunID, sent[2]["value"])
}

func TestTriggerDagRunLinkUsesTheBaseURL(t *testing.T) {
	triggerTestEnv(t, map[string]string{apiBaseURLEnv: "https://airflow.example.com//"})
	supervisor := &triggerSupervisor{}

	runTriggerTask(t, bundle.TriggerSpec{DagID: "downstream", RunID: "r1"}, supervisor, nil)

	assert.Equal(t,
		"https://airflow.example.com/dags/downstream/runs/r1",
		supervisor.sent()[0]["value"],
	)
}

func TestTriggerDagRunSendsTheOptionsAsWritten(t *testing.T) {
	triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{}
	spec := bundle.TriggerSpec{
		DagID:       "downstream",
		RunID:       "downstream_x",
		Note:        "{{ ds }}",
		Conf:        map[string]any{"day": "{{ ds }}", "rows": int64(2)},
		ResetDagRun: true,
	}

	result := runTriggerTask(t, spec, supervisor, nil)

	assertSucceedTask(t, result)
	sent := supervisor.request(t, "TriggerDagRun")
	assert.Equal(t, "downstream_x", sent["run_id"], "an explicit run_id is not generated")
	assert.Equal(t, "{{ ds }}", sent["note"])
	assert.Equal(t, map[string]any{"day": "{{ ds }}", "rows": int8(2)}, sent["conf"])
	assert.Equal(t, true, sent["reset_dag_run"])
	assert.Equal(t, "downstream_x", supervisor.sent()[2]["value"])
}

func TestTriggerDagRunDatesAndRunID(t *testing.T) {
	custom := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	runAfter := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	tests := []struct {
		name            string
		spec            bundle.TriggerSpec
		wantLogicalDate any
		wantRunAfter    any
		wantRunID       string
	}{
		{
			name:            "neither set",
			spec:            bundle.TriggerSpec{DagID: "d"},
			wantLogicalDate: triggerNow,
			wantRunID:       triggerNowRunID,
		},
		{
			name:         "only RunAfter set",
			spec:         bundle.TriggerSpec{DagID: "d", RunAfter: runAfter},
			wantRunAfter: runAfter,
			wantRunID:    "manual__2026-02-03T04:05:06+00:00_AbCd1234",
		},
		{
			name:            "only LogicalDate set",
			spec:            bundle.TriggerSpec{DagID: "d", LogicalDate: custom},
			wantLogicalDate: custom,
			wantRunID:       "manual__2026-01-02T03:04:05+00:00",
		},
		{
			name: "both set",
			spec: bundle.TriggerSpec{
				DagID:       "d",
				LogicalDate: custom,
				RunAfter:    runAfter,
			},
			wantLogicalDate: custom,
			wantRunAfter:    runAfter,
			wantRunID:       "manual__2026-02-03T04:05:06+00:00",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggerTestEnv(t, nil)
			supervisor := &triggerSupervisor{}

			runTriggerTask(t, tc.spec, supervisor, nil)

			sent := supervisor.request(t, "TriggerDagRun")
			assert.Equal(t, tc.wantRunID, sent["run_id"])
			for key, want := range map[string]any{
				"logical_date": tc.wantLogicalDate, "run_after": tc.wantRunAfter,
			} {
				if want == nil {
					assert.NotContains(t, sent, key)
					continue
				}
				got, ok := sent[key].(time.Time)
				require.True(t, ok, "%s is %T", key, sent[key])
				assert.True(t, want.(time.Time).Equal(got), "%s is %v, want %v", key, got, want)
			}
		})
	}
}

func TestTriggerDagRunAlreadyExists(t *testing.T) {
	tests := []struct {
		name string
		skip bool
		want genmodels.TaskStateState
	}{
		{"skipped", true, genmodels.TaskStateStateSkipped},
		{"failed", false, genmodels.TaskStateStateFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggerTestEnv(t, nil)
			supervisor := &triggerSupervisor{triggerErr: "DAGRUN_ALREADY_EXISTS"}
			spec := bundle.TriggerSpec{DagID: "downstream", SkipWhenAlreadyExists: tc.skip}

			// A task that may retry still ends in this state, as in Python.
			result := runTriggerTask(t, spec, supervisor, shouldRetry)

			assertTaskState(t, result, tc.want)
			assert.Equal(t,
				[]string{"SetXCom:_link_TriggerDagRunLink", "TriggerDagRun"},
				supervisor.types(), "no trigger_run_id is pushed for a run that was not created",
			)
		})
	}
}

func TestTriggerDagRunErrorRetries(t *testing.T) {
	triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{triggerErr: "API_SERVER_ERROR"}

	result := runTriggerTask(t, bundle.TriggerSpec{DagID: "downstream"}, supervisor, shouldRetry)
	assertRetryTask(t, result, "API_SERVER_ERROR")

	result = runTriggerTask(t, bundle.TriggerSpec{DagID: "downstream"}, supervisor, nil)
	assertTaskState(t, result, genmodels.TaskStateStateFailed)
}

func TestTriggerDagRunFailsWhenTheDagIsPaused(t *testing.T) {
	triggerTestEnv(t, nil)
	spec := bundle.TriggerSpec{DagID: "downstream", FailWhenDagIsPaused: true}

	paused := &triggerSupervisor{paused: true}
	assertRetryTask(t, runTriggerTask(t, spec, paused, shouldRetry), "Dag downstream is paused")
	assert.Equal(t, []string{"GetDag"}, paused.types(), "nothing is triggered")

	running := &triggerSupervisor{}
	assertSucceedTask(t, runTriggerTask(t, spec, running, nil))
	assert.Equal(t, "GetDag", running.types()[0])
	assert.Contains(t, running.types(), "TriggerDagRun")

	ignored := &triggerSupervisor{paused: true}
	spec.FailWhenDagIsPaused = false
	assertSucceedTask(t, runTriggerTask(t, spec, ignored, nil))
	assert.NotContains(t, ignored.types(), "GetDag")
}

func TestTriggerDagRunPollsUntilAnAllowedState(t *testing.T) {
	slept := triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{states: []string{"queued", "running", "success"}}
	spec := bundle.TriggerSpec{DagID: "downstream", WaitForCompletion: true}

	result := runTriggerTask(t, spec, supervisor, nil)

	assertSucceedTask(t, result)
	assert.Equal(t, []time.Duration{60 * time.Second, 60 * time.Second, 60 * time.Second}, *slept)
	assert.Equal(t, triggerNowRunID, supervisor.request(t, "GetDagRunState")["run_id"])
}

func TestTriggerDagRunPollFailsOnAFailedState(t *testing.T) {
	slept := triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{states: []string{"running", "failed"}}
	poke := 5 * time.Second
	spec := bundle.TriggerSpec{DagID: "downstream", WaitForCompletion: true, PokeInterval: &poke}

	result := runTriggerTask(t, spec, supervisor, shouldRetry)

	assertRetryTask(t, result, "downstream failed with failed state failed")
	assert.Equal(t, []time.Duration{poke, poke}, *slept)

	supervisor = &triggerSupervisor{states: []string{"failed"}}
	assertTaskState(t, runTriggerTask(t, spec, supervisor, nil), genmodels.TaskStateStateFailed)
}

func TestTriggerDagRunPollHonorsTheStatesOfTheTask(t *testing.T) {
	triggerTestEnv(t, nil)
	// No state fails the task, so the failed Dag run is not an allowed state to wait out.
	supervisor := &triggerSupervisor{states: []string{"failed", "queued", "failed", "running"}}
	spec := bundle.TriggerSpec{
		DagID:             "downstream",
		WaitForCompletion: true,
		AllowedStates:     []string{"running"},
		FailedStates:      []string{},
	}

	result := runTriggerTask(t, spec, supervisor, nil)

	assertSucceedTask(t, result)
	assert.Equal(t, 4, len(supervisor.types())-2-1, "four polls after the link, trigger and run_id")
}

func TestTriggerDagRunPollStopsWhenTheTaskIsCancelled(t *testing.T) {
	triggerTestEnv(t, nil)
	defaultTriggerEnv.sleep = func(ctx context.Context, d time.Duration) error {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
	supervisor := &triggerSupervisor{states: []string{"running"}}
	poke := time.Hour
	spec := bundle.TriggerSpec{DagID: "downstream", WaitForCompletion: true, PokeInterval: &poke}
	b := testBundle{"test_dag": testDag{"trigger": &bundle.TriggerTask{Spec: spec}}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)

	done := make(chan any, 1)
	go func() {
		done <- RunTask(ctx, b, newStartupDetails("trigger"), supervisor.comm(t), logger)
	}()

	select {
	case result := <-done:
		assertTaskState(t, result, genmodels.TaskStateStateFailed)
	case <-time.After(5 * time.Second):
		t.Fatal("the task kept sleeping after its context was cancelled")
	}
	assert.NotContains(t, supervisor.types(), "GetDagRunState")
}

func TestTriggerDagRunDefers(t *testing.T) {
	poke := 90 * time.Second
	tests := []struct {
		name       string
		settings   map[string]string
		spec       bundle.TriggerSpec
		wantQueue  any
		wantStates []any
		wantPoll   any
	}{
		{
			name:       "defaults, queues off",
			spec:       bundle.TriggerSpec{Deferrable: ptr(true)},
			wantStates: []any{"success", "failed"},
			wantPoll:   int8(60),
		},
		{
			name:       "queues on",
			settings:   map[string]string{triggererQueuesEnabledEnv: "True"},
			spec:       bundle.TriggerSpec{Deferrable: ptr(true)},
			wantQueue:  "golang",
			wantStates: []any{"success", "failed"},
			wantPoll:   int8(60),
		},
		{
			name: "default_deferrable, own states and interval",
			settings: map[string]string{
				defaultDeferrableEnv:      "True",
				triggererQueuesEnabledEnv: "False",
			},
			spec: bundle.TriggerSpec{
				PokeInterval:  &poke,
				AllowedStates: []string{"success", "queued"},
				FailedStates:  []string{"failed"},
			},
			wantStates: []any{"success", "queued", "failed"},
			wantPoll:   int8(90),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			slept := triggerTestEnv(t, tc.settings)
			supervisor := &triggerSupervisor{}
			tc.spec.DagID = "downstream"
			tc.spec.WaitForCompletion = true

			result := runTriggerTask(t, tc.spec, supervisor, nil)

			assert.Empty(t, *slept, "a deferred task does not poll")
			assert.NotContains(t, supervisor.types(), "GetDagRunState")
			deferred, ok := result.(genmodels.DeferTask)
			require.True(t, ok, "expected DeferTask, got %T", result)
			wire, err := encodeRequest(0, deferred)
			require.NoError(t, err)
			var framed bytes.Buffer
			require.NoError(t, writeFrame(&framed, wire))
			frame, err := readFrame(&framed)
			require.NoError(t, err)
			want := map[string]any{
				"type":        "DeferTask",
				"state":       "deferred",
				"classpath":   "airflow.providers.standard.triggers.external_task.DagStateTrigger",
				"next_method": "execute_complete",
				"next_kwargs": map[string]any{},
				"trigger_kwargs": map[string]any{
					"dag_id":          "downstream",
					"states":          tc.wantStates,
					"poll_interval":   tc.wantPoll,
					"run_ids":         []any{triggerNowRunID},
					"execution_dates": nil,
				},
			}
			if tc.wantQueue != nil {
				want["queue"] = tc.wantQueue
			}
			assert.Equal(t, want, rawToMap(t, frame.Body))
		})
	}
}

func TestTriggerDagRunDeferrableFalseOverridesTheConfig(t *testing.T) {
	slept := triggerTestEnv(t, map[string]string{defaultDeferrableEnv: "True"})
	supervisor := &triggerSupervisor{states: []string{"success"}}
	spec := bundle.TriggerSpec{DagID: "downstream", WaitForCompletion: true, Deferrable: ptr(false)}

	result := runTriggerTask(t, spec, supervisor, nil)

	assertSucceedTask(t, result)
	assert.Len(t, *slept, 1, "the task polled instead of deferring")
}

func TestTriggerDagRunIgnoresDeferrableWithoutWaiting(t *testing.T) {
	triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{}

	result := runTriggerTask(t,
		bundle.TriggerSpec{DagID: "downstream", Deferrable: ptr(true)}, supervisor, nil,
	)

	assertSucceedTask(t, result)
}

func TestTriggerDagRunRejectsAnInvalidSetting(t *testing.T) {
	triggerTestEnv(t, map[string]string{defaultDeferrableEnv: "maybe"})
	supervisor := &triggerSupervisor{}

	result := runTriggerTask(t, bundle.TriggerSpec{DagID: "downstream"}, supervisor, shouldRetry)

	assertRetryTask(t, result, defaultDeferrableEnv)
}

func TestTriggerDagRunResume(t *testing.T) {
	tuple := func(data map[string]any) map[string]any {
		return map[string]any{
			"__classname__": "builtins.tuple",
			"__data__":      []any{dagStateTriggerClasspath, data},
		}
	}
	tests := []struct {
		name       string
		nextMethod string
		kwargs     map[string]any
		// wantRetry is a substring of the retry reason, or empty when the task succeeds.
		wantRetry string
	}{
		{
			name:       "allowed state",
			nextMethod: "execute_complete",
			kwargs: map[string]any{
				"event": tuple(map[string]any{"run_ids": []any{"r1"}, "r1": "success"}),
			},
		},
		{
			name:       "failed state",
			nextMethod: "execute_complete",
			kwargs: map[string]any{
				"event": tuple(
					map[string]any{"run_ids": []any{"r1", "r2"}, "r1": "success", "r2": "failed"},
				),
			},
			wantRetry: `downstream failed with failed states ["failed"] for run_ids ["r2"]`,
		},
		{
			name:       "event without the tuple wrapper",
			nextMethod: "execute_complete",
			kwargs: map[string]any{
				"event": []any{
					dagStateTriggerClasspath,
					map[string]any{"run_ids": []any{"r1"}, "r1": "failed"},
				},
			},
			wantRetry: "for run_ids",
		},
		{
			name:       "event that cannot be read",
			nextMethod: "execute_complete",
			kwargs:     map[string]any{"event": "oops"},
			wantRetry:  `Task resumed with an event it cannot read: "oops"`,
		},
		{
			name:       "no event",
			nextMethod: "execute_complete",
			wantRetry:  "Task resumed with an event it cannot read: null",
		},
		{
			name:       "trigger failed",
			nextMethod: "__fail__",
			kwargs:     map[string]any{"error": "trigger crashed", "traceback": []any{"line 1"}},
			wantRetry:  "trigger crashed",
		},
		{
			name:       "trigger failed without an error",
			nextMethod: "__fail__",
			wantRetry:  "Unknown",
		},
		{
			name:       "unknown method",
			nextMethod: "other",
			wantRetry:  `Task cannot resume with next_method "other"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggerTestEnv(t, nil)
			supervisor := &triggerSupervisor{}
			resume := func(details *genmodels.StartupDetails) {
				details.TIContext.ShouldRetry = true
				details.TIContext.NextMethod = tc.nextMethod
				if tc.kwargs != nil {
					kwargs := genmodels.NextKwargs(tc.kwargs)
					details.TIContext.NextKwargs = &kwargs
				}
			}

			result := runTriggerTask(t, bundle.TriggerSpec{DagID: "downstream"}, supervisor, resume)

			if tc.wantRetry == "" {
				assertSucceedTask(t, result)
			} else {
				assertRetryTask(t, result, tc.wantRetry)
			}
			assert.Empty(t, supervisor.types(), "a resumed task triggers nothing")
		})
	}
}

func TestTriggerDagRunResumeUsesTheStatesOfTheTask(t *testing.T) {
	triggerTestEnv(t, nil)
	supervisor := &triggerSupervisor{}
	spec := bundle.TriggerSpec{DagID: "downstream", FailedStates: []string{"queued"}}
	kwargs := genmodels.NextKwargs{"event": []any{
		dagStateTriggerClasspath, map[string]any{"run_ids": []any{"r1"}, "r1": "failed"},
	}}

	result := runTriggerTask(t, spec, supervisor, func(details *genmodels.StartupDetails) {
		details.TIContext.NextMethod = "execute_complete"
		details.TIContext.NextKwargs = &kwargs
	})

	assertSucceedTask(t, result)
}

func TestTriggerTaskExecuteIsNeverCalled(t *testing.T) {
	err := (&bundle.TriggerTask{}).Execute(context.Background(), nil, nil)
	assert.ErrorContains(t, err, "run by the runtime")
}
