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
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/suite"
	"github.com/swaggest/assertjson"

	"github.com/apache/airflow/go-sdk/pkg/api"
)

const ExecutionAPIServer = "http://localhost:9999/execution"

var TestWorkload = api.ExecuteTaskWorkload{
	Token: "",
	// {"context_carrier":{},"dag_id":"tutorial_dag","hostname":null,"id":"0196ab8a-5c97-7d4f-b431-e3f49ce20b7f","map_index":-1,"run_id":"manual__2025-05-07T15:48:39.420678+00:00","task_id":"extract","try_number":5}
	TI: api.TaskInstance{
		ContextCarrier: new(map[string]any),
		DagId:          "tutorial_dag",
		RunId:          "manual__2025-05-07T15:48:39.420678+00:00",
		TaskId:         "extract",
		Id:             uuid.MustParse("0196ab8a-5c97-7d4f-b431-e3f49ce20b7f"),
		// MapIndex:       new(int),
		TryNumber: 1,
	},
	BundleInfo: api.BundleInfo{
		Name:    "example_dags",
		Version: nil,
	},
	// LogPath: new(string),
}

func init() {
	idx := -1
	log := "dag_id=tutorial_dag/run_id=manual__2025-05-07T15:48:39.420678+00:00/task_id=extract/attempt=5.log"
	TestWorkload.TI.MapIndex = &idx
	TestWorkload.LogPath = &log
}

type WorkerSuite struct {
	suite.Suite
	worker    Worker
	transport *httpmock.MockTransport
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, &WorkerSuite{})
}

func (s *WorkerSuite) SetupTest() {
	s.worker = New(slog.Default())

	s.transport = httpmock.NewMockTransport()
	client, err := api.NewClient(ExecutionAPIServer, api.WithRoundTripper(s.transport))
	s.Require().NoError(err)

	s.worker.(*worker).client = client
}

func (s *WorkerSuite) TestWithServer() {
	s.worker.(*worker).heartbeatInterval = 100 * time.Millisecond
	iface, err := s.worker.WithServer("http://example.com")

	s.Require().NoError(err)
	w := iface.(*worker)
	s.Equal(100*time.Millisecond, w.heartbeatInterval)
	s.Equal(w.client.(*api.Client).BaseURL(), "http://example.com")
}

// ExpectTaskRun sets up  a matcher for the "/task-instances/{id}/run" end point and adds a finalize check
// that it has been called
func (s *WorkerSuite) ExpectTaskRun() {
	s.transport.RegisterResponder(
		http.MethodPatch,
		"=~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/run",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{}),
	)
	s.T().Cleanup(func() {
		callCounts := s.transport.GetCallCountInfo()

		s.Equal(callCounts["PATCH =~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/run"], 1)
	})
}

// ExpectTaskState sets up a matcher for the "/task-instances/{id}/state" with the given state end point and adds a finalize check
// that it has been called
func (s *WorkerSuite) ExpectTaskState(state any) {
	s.transport.RegisterMatcherResponder(
		http.MethodPatch,
		"=~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/state",
		s.BodyJSONMatches(fmt.Appendf(nil, `{"state": %q}`, state)),
		httpmock.NewJsonResponderOrPanic(200, map[string]any{}),
	)
	s.T().Cleanup(func() {
		callCounts := s.transport.GetCallCountInfo()

		s.Equal(
			callCounts["PATCH =~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/state <BodyJSONMatches>"],
			1,
			"Actual call counts: %#v",
			callCounts,
		)
	})
}

// BodyContainsJSON creates an httpmock Matcher that will check that the http.Request body contains the given
// JSON fields
//
// The request can contain extra JSON fields. See [github.com/swaggest/assertjson.Matches] for more info
func (s *WorkerSuite) BodyJSONMatches(expected []byte) httpmock.Matcher {
	matcher := httpmock.NewMatcher("BodyJSONMatches", func(req *http.Request) bool {
		b, err := io.ReadAll(req.Body)
		return err == nil && assertjson.Matches(s.T(), expected, b)
	})
	return matcher
}

// TestTaskNotRegisteredErrors checks that when a task cannot be found we report "success" on the Workload but
// report the task as failed to the Execution API server
func (s *WorkerSuite) TestTaskNotRegisteredErrors() {
	s.transport.RegisterMatcherResponder(
		http.MethodPatch,
		"=~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/state",
		s.BodyJSONMatches([]byte(`{"state": "failed"}`)),
		httpmock.NewJsonResponderOrPanic(200, map[string]any{}),
	)
	err := s.worker.ExecuteTaskWorkload(context.Background(), TestWorkload)

	s.NoError(err, "ExecuteTaskWorkload should not report an error")

	s.Equal(1, s.transport.GetTotalCallCount(), "State was reported to server")
}

// TestStartContextErrorTaskDoesntStart checks that if the /run endpoint returns an error that task doesn't
// start, but that it is logged
func (s *WorkerSuite) TestStartContextErrorTaskDoesntStart() {
	s.T().Skip("TODO: Not implemented yet")
}

// TestTaskPanicReportsFailedState tests that when the task/user code panics that we catch it and report thr
// error upstream
func (s *WorkerSuite) TestTaskPanicReportsFailedState() {
	s.T().Skip("TODO: Not implemented yet")
}

func (s *WorkerSuite) TestTaskReturnErrorReportsFailedState() {
	s.T().Skip("TODO: Not implemented yet")
}

func (s *WorkerSuite) TestTaskHeartbeatsWhileRunning() {
	s.worker.RegisterTaskWithName("tutorial_dag", "extract", func() error {
		time.Sleep(time.Second)
		return nil
	})

	s.ExpectTaskRun()
	s.ExpectTaskState(api.TerminalTIStateSuccess)
	s.transport.RegisterResponder(
		http.MethodPut,
		"=~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/heartbeat",
		httpmock.NewJsonResponderOrPanic(200, map[string]any{}),
	)

	s.worker.(*worker).heartbeatInterval = 100 * time.Millisecond
	err := s.worker.ExecuteTaskWorkload(context.Background(), TestWorkload)
	s.NoError(err, "ExecuteTaskWorkload should not report an error")

	callCounts := s.transport.GetCallCountInfo()

	// Since we heartbeat every 100ms and run for 1 second, we should expect 10 heartbeat calls. But allow +/-
	// 1 due to timing imprecision
	s.InDelta(
		10,
		callCounts["PUT =~^"+ExecutionAPIServer+"/task-instances/[0-9a-f-]{36}/heartbeat"],
		1,
		"Actual call counts: %#v", callCounts,
	)
}

func (s *WorkerSuite) TestTaskHeatbeatErrorStopsTaskAndLogs() {
	s.T().Skip("TODO: Not implemented yet")
}

func (s *WorkerSuite) TestTokenRefreshHeaderRespected() {
	s.T().Skip("TODO: Not implemented yet")
}
