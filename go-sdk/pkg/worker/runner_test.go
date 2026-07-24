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

package worker_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cappuccinotm/slogx/slogt"
	"github.com/google/uuid"
	"github.com/jarcoal/httpmock"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"resty.dev/v3"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/api/mocks"
	"github.com/apache/airflow/go-sdk/pkg/worker"
)

const ExecutionAPIServer = "http://localhost:9999/execution"

func newTestWorkLoad(id string, dagId string) api.ExecuteTaskWorkload {
	if dagId == "" {
		dagId = "tutorial_dag"
	}
	idx := -1
	log := fmt.Sprintf(
		"dag_id=%s/run_id=manual__2025-05-07T15:48:39.420678+00:00/task_id=extract/attempt=5.log",
		dagId,
	)
	return api.ExecuteTaskWorkload{
		Token: "",
		// {"context_carrier":{},"dag_id":"tutorial_dag","hostname":null,"id":"0196ab8a-5c97-7d4f-b431-e3f49ce20b7f","map_index":-1,"run_id":"manual__2025-05-07T15:48:39.420678+00:00","task_id":"extract","try_number":5}
		TI: api.TaskInstance{
			ContextCarrier: new(map[string]any),
			DagId:          dagId,
			RunId:          "manual__2025-05-07T15:48:39.420678+00:00",
			TaskId:         "extract",
			Id:             uuid.MustParse(id),
			MapIndex:       &idx,
			TryNumber:      1,
		},
		BundleInfo: api.BundleInfo{
			Name:    "example_dags",
			Version: nil,
		},
		LogPath: &log,
	}
}

func TestStartTaskHTTPErrorDoesNotLogToken(t *testing.T) {
	secretToken := "super-secret-workload-token"
	workload := newTestWorkLoad(uuid.New().String(), "start-error-dag")
	workload.Token = secretToken
	workload.LogPath = nil
	request, err := http.NewRequest(
		http.MethodPatch,
		"https://api.example.test/execution/task-instances/"+workload.TI.Id.String()+"/run?access_token="+secretToken,
		nil,
	)
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+secretToken)
	request.Header.Set("Correlation-Id", "test-correlation-id")

	httpError := &api.GeneralHTTPError{Response: &resty.Response{
		Request: &resty.Request{
			Header:     request.Header,
			Method:     request.Method,
			RawRequest: request,
		},
		RawResponse: &http.Response{StatusCode: http.StatusBadRequest},
	}}
	registry := bundlev1.New()
	registry.AddDag(workload.TI.DagId).AddTaskWithName(workload.TI.TaskId, func() error {
		return nil
	})
	client := &mocks.ClientInterface{}
	taskInstances := &mocks.TaskInstancesClient{}
	taskInstances.EXPECT().Run(mock.Anything, workload.TI.Id, mock.Anything).Return(nil, httpError)
	client.EXPECT().TaskInstances().Return(taskInstances)

	var logBuffer bytes.Buffer
	testWorker := worker.NewWithBundle(registry, slog.New(slog.NewJSONHandler(&logBuffer, nil))).
		WithClient(client)
	err = testWorker.ExecuteTaskWorkload(context.Background(), workload)
	require.Error(t, err)
	client.AssertExpectations(t)
	taskInstances.AssertExpectations(t)
	require.NotContains(t, logBuffer.String(), secretToken)

	var logEntry struct {
		Level      string `json:"level"`
		Message    string `json:"msg"`
		StatusCode int    `json:"status_code"`
		Request    struct {
			CorrelationID string `json:"correlation_id"`
			Method        string `json:"method"`
			Path          string `json:"path"`
		} `json:"request"`
	}
	require.NoError(t, json.Unmarshal(logBuffer.Bytes(), &logEntry))
	require.Equal(t, "ERROR", logEntry.Level)
	require.Equal(t, "Server reported error when attempting to start task", logEntry.Message)
	require.Equal(t, http.StatusBadRequest, logEntry.StatusCode)
	require.Equal(t, request.Header.Get("Correlation-Id"), logEntry.Request.CorrelationID)
	require.Equal(t, request.Method, logEntry.Request.Method)
	require.Equal(t, request.URL.Path, logEntry.Request.Path)
}

type WorkerSuite struct {
	suite.Suite
	worker    worker.Worker
	registry  bundlev1.Registry
	client    *mocks.ClientInterface
	ti        *mocks.TaskInstancesClient
	transport *httpmock.MockTransport
}

func TestWorkerSuite(t *testing.T) {
	suite.Run(t, &WorkerSuite{})
}

func (s *WorkerSuite) SetupSuite() {
	// Stop the test from writing log files
	viper.Set("logging.task.stdout_only", "true")

	s.registry = bundlev1.New()
	s.worker = worker.NewWithBundle(s.registry, slog.New(slogt.Handler(s.T())))

	s.transport = httpmock.NewMockTransport()
	s.client = &mocks.ClientInterface{}
	s.ti = &mocks.TaskInstancesClient{}
	s.worker = s.worker.WithHeartbeatInterval(100 * time.Millisecond).WithClient(s.client)
}

func (s *WorkerSuite) TearDownSuite() {
	s.ti.AssertExpectations(s.T())
	s.client.AssertExpectations(s.T())
}

// ExpectTaskRun sets up  a matcher for the "/task-instances/{id}/run" end point and adds a finalize check
// that it has been called
func (s *WorkerSuite) ExpectTaskRun(taskId string) {
	s.T().Helper()
	s.ti.EXPECT().
		Run(mock.Anything, uuid.MustParse(taskId), mock.Anything).
		Return(&api.TIRunContext{}, nil)
	s.client.EXPECT().TaskInstances().Return(s.ti)
}

// ExpectTaskState sets up a matcher for the "/task-instances/{id}/state" with the given state end point
func (s *WorkerSuite) ExpectTaskState(taskId string, state api.TerminalTIState) {
	s.T().Helper()
	s.ti.EXPECT().
		UpdateState(mock.AnythingOfType("context.backgroundCtx"), uuid.MustParse(taskId), mock.AnythingOfType("*api.TIUpdateStatePayload")).
		RunAndReturn(func(ctx context.Context, taskInstanceId uuid.UUID, body *api.TIUpdateStatePayload) error {
			if payload, err := body.AsTITerminalStatePayload(); err == nil {
				if payload.State == api.TerminalStateNonSuccess(state) {
					return nil
				}
			} else {
				payload, err := body.AsTISuccessStatePayload()
				if err == nil && payload.State == api.TISuccessStatePayloadState(state) {
					return nil
				}
			}
			return fmt.Errorf("Error")
		}).
		Once()

	s.client.EXPECT().TaskInstances().Return(s.ti)
}

// TestTaskNotRegisteredErrors checks that when a task cannot be found we report "success" on the Workload but
// report the task as failed to the Execution API server
func (s *WorkerSuite) TestTaskNotRegisteredErrors() {
	id := uuid.New().String()
	testWorkload := newTestWorkLoad(id, id[:8])
	s.ExpectTaskState(id, api.TerminalTIStateFailed)
	err := s.worker.ExecuteTaskWorkload(context.Background(), testWorkload)

	s.NoError(
		err,
		"ExecuteTaskWorkload should not report an error %#v",
		s.transport.GetCallCountInfo(),
	)
}

// TestStartContextErrorTaskDoesntStart checks that if the /run endpoint returns an error that task doesn't
// start, but that it is logged
func (s *WorkerSuite) TestStartContextErrorTaskDoesntStart() {
	id := uuid.New().String()
	testWorkload := newTestWorkLoad(id, id[:8])

	// Flag to see if the Task gets called
	wasCalled := false

	// Register a task that should NOT be called if everything works
	s.registry.AddDag(testWorkload.TI.DagId).AddTaskWithName(testWorkload.TI.TaskId, func() error {
		wasCalled = true
		return nil
	})

	// Setup the mock
	s.ti.EXPECT().
		Run(mock.Anything, uuid.MustParse(id), mock.Anything).
		Return(nil, fmt.Errorf("simulated start context error"))

	s.client.EXPECT().TaskInstances().Return(s.ti)

	err := s.worker.ExecuteTaskWorkload(context.Background(), testWorkload)

	s.Error(err)
	s.Contains(err.Error(), "simulated start context error")
	s.False(wasCalled, "Task function should not be executed when start context fails")
}

// TestTaskPanicReportsFailedState tests that when the task/user code panics that we catch it and report the
// error upstream
func (s *WorkerSuite) TestTaskPanicReportsFailedState() {
	s.T().Skip("TODO: Not implemented yet")
}

func (s *WorkerSuite) TestTaskReturnErrorReportsFailedState() {
	s.T().Skip("TODO: Not implemented yet")
}

func (s *WorkerSuite) TestTaskHeartbeatsWhileRunning() {
	id := uuid.New().String()
	var callCount atomic.Int32
	testWorkload := newTestWorkLoad(id, id[:8])
	s.registry.AddDag(testWorkload.TI.DagId).AddTaskWithName(testWorkload.TI.TaskId, func() error {
		time.Sleep(time.Second)
		return nil
	})

	s.ExpectTaskRun(id)
	s.ExpectTaskState(id, api.TerminalTIStateSuccess)
	s.ti.EXPECT().
		Heartbeat(mock.Anything, uuid.MustParse(id), mock.Anything).
		RunAndReturn(func(ctx context.Context, taskInstanceId uuid.UUID, body *api.TIHeartbeatInfo) error {
			if taskInstanceId.String() == id {
				callCount.Add(1)
			}
			return nil
		})
	s.client.EXPECT().TaskInstances().Return(s.ti)

	err := s.worker.ExecuteTaskWorkload(context.Background(), testWorkload)
	s.NoError(err, "ExecuteTaskWorkload should not report an error")

	// Since we heartbeat every 100ms and run for 1 second, we should expect 10 heartbeat calls. But allow +/-
	// 1 due to timing imprecision
	count := callCount.Load()
	s.Assert().
		True(count <= 11 && count >= 9, fmt.Sprintf("Call count of %d was not within the margin of error of 10+/-1", count))
}

func (s *WorkerSuite) TestTaskHeartbeatConflictStopsTask() {
	id := uuid.New().String()
	testWorkload := newTestWorkLoad(id, id[:8])

	s.registry.AddDag(testWorkload.TI.DagId).
		AddTaskWithName(testWorkload.TI.TaskId, func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
				return fmt.Errorf("task context was not cancelled")
			}
		})

	s.ExpectTaskRun(id)
	s.ExpectTaskState(id, api.TerminalTIStateFailed)
	s.ti.EXPECT().
		Heartbeat(mock.Anything, uuid.MustParse(id), mock.Anything).
		Return(&api.GeneralHTTPError{
			Response: &resty.Response{
				RawResponse: &http.Response{
					Status:     "409 Conflict",
					StatusCode: http.StatusConflict,
				},
			},
		})
	s.client.EXPECT().TaskInstances().Return(s.ti)

	err := s.worker.ExecuteTaskWorkload(context.Background(), testWorkload)
	s.NoError(err)
}

func (s *WorkerSuite) TestTaskHeartbeatErrorStopsTaskAndLogs() {
	s.T().Skip("TODO: Not implemented yet")
}

func (s *WorkerSuite) TestTokenRefreshHeaderRespected() {
	s.T().Skip("TODO: Not implemented yet")
}
