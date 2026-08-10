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

package concurrentxcom

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/airflow/go-sdk/pkg/api"
	"github.com/apache/airflow/go-sdk/sdk"
)

// mockXComClient is a mutex-guarded in-memory sdk.Client so the concurrent
// GetXCom goroutines are race-free under `go test -race`.
type mockXComClient struct {
	mu     sync.RWMutex
	values map[string]any
}

func newMockXComClient() *mockXComClient {
	return &mockXComClient{values: make(map[string]any)}
}

func (m *mockXComClient) PushXCom(
	ctx context.Context,
	ti api.TaskInstance,
	key string,
	value any,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[key] = value
	return nil
}

func (m *mockXComClient) GetXCom(
	ctx context.Context,
	dagId, runId, taskId string,
	mapIndex *int,
	key string,
	value any,
) (any, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.values[key]
	if !ok {
		return nil, sdk.XComNotFound
	}
	return v, nil
}

func (m *mockXComClient) GetVariable(ctx context.Context, key string) (string, error) {
	panic("unimplemented")
}

func (m *mockXComClient) UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error {
	panic("unimplemented")
}

func (m *mockXComClient) GetConnection(ctx context.Context, connID string) (sdk.Connection, error) {
	panic("unimplemented")
}

var _ sdk.Client = (*mockXComClient)(nil)

func Test_PullXComsConcurrently(t *testing.T) {
	ctx := sdk.NewTIRunContext(
		context.Background(),
		sdk.TaskInstance{
			DagID:  "concurrent_xcom_dag",
			RunID:  "run",
			TaskID: "pull_xcoms_concurrently",
		},
		sdk.DagRun{},
	)

	result, err := PullXComsConcurrently(ctx, newMockXComClient(), slog.Default())
	assert.NoError(t, err)

	m, ok := result.(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, numXComs, m["num_xcoms"])

	sequential := m["sequential_ms"].(int64)
	concurrent := m["concurrent_ms"].(int64)
	assert.Greater(t, sequential, int64(0))
	assert.Greater(t, concurrent, int64(0))
	// The per-item work overlaps across goroutines, so concurrent beats sequential.
	assert.Less(t, concurrent, sequential)
}
