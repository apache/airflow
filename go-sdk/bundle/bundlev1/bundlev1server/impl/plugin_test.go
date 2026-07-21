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

package impl

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	proto "github.com/apache/airflow/go-sdk/internal/protov1"
	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/pkg/sdkcontext"
)

type testBundleProvider struct {
	task func(context.Context) error
}

func (*testBundleProvider) GetBundleVersion() bundlev1.BundleInfo {
	return bundlev1.BundleInfo{Name: "test-bundle"}
}

func (p *testBundleProvider) RegisterDags(registry bundlev1.Registry) error {
	registry.AddDag("test-dag").AddTaskWithName("test-task", p.task)
	return nil
}

func TestBuildContextCarrier(t *testing.T) {
	actual := buildContextCarrier(map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	})

	require.NotNil(t, actual)
	assert.Equal(t, map[string]any{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	}, *actual)
}

func TestExecutePropagatesOTelContextToWorkload(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPatch && r.URL.Path == "/task-instances/00000000-0000-0000-0000-000000000001/run":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
		case r.Method == http.MethodPatch && r.URL.Path == "/task-instances/00000000-0000-0000-0000-000000000001/state":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(apiServer.Close)
	viper.Set("execution.api_url", apiServer.URL)
	t.Cleanup(func() { viper.Set("execution.api_url", "") })

	otelContext := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	}
	var observedCarrier *map[string]any
	provider := &testBundleProvider{task: func(ctx context.Context) error {
		workload := ctx.Value(sdkcontext.WorkloadContextKey).(api.ExecuteTaskWorkload)
		observedCarrier = workload.TI.ContextCarrier
		return nil
	}}
	taskInstanceID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	taskInstanceIDString := taskInstanceID.String()
	dagID := "test-dag"
	runID := "test-run"
	taskID := "test-task"
	tryNumber := int32(1)
	bundleName := "test-bundle"
	token := "test-token"
	request := proto.Execute_Request_builder{
		Task: proto.ExecuteTaskWorkload_builder{
			BundleInfo: proto.BundleInfo_builder{Name: &bundleName}.Build(),
			Ti: proto.TaskInstance_builder{
				Id:          proto.UUID_builder{Value: &taskInstanceIDString}.Build(),
				DagId:       &dagID,
				RunId:       &runID,
				TaskId:      &taskID,
				TryNumber:   &tryNumber,
				OtelContext: otelContext,
			}.Build(),
			Token: &token,
		}.Build(),
	}.Build()

	_, err := (&server{Impl: provider}).Execute(t.Context(), request)

	require.NoError(t, err)
	require.NotNil(t, observedCarrier)
	assert.Equal(t, map[string]any{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	}, *observedCarrier)
}
