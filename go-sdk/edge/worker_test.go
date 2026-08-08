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

package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/pkg/bundles/shared"
	"github.com/apache/airflow/go-sdk/pkg/edgeapi"
)

func TestFetchJobDoesNotLogToken(t *testing.T) {
	var logBuffer bytes.Buffer

	secretToken := "super-secret-edge-token"
	version := "1.0.0"
	logPath := "/tmp/example.log"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/edge_worker/v1/jobs/fetch/test-worker", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(edgeapi.EdgeJobFetched{
			Command: edgeapi.ExecuteTask{
				BundleInfo: edgeapi.BundleInfo{
					Name:    "example-bundle",
					Version: &version,
				},
				DagRelPath: "dags/example.py",
				LogPath:    &logPath,
				Ti: edgeapi.TaskInstance{
					DagId:          "example_dag",
					Queue:          "default",
					RunId:          "manual__2026-06-10T00:00:00+00:00",
					TaskId:         "example_task",
					TryNumber:      2,
					PoolSlots:      1,
					PriorityWeight: 1,
				},
				Token: secretToken,
			},
			ConcurrencySlots: 3,
			DagId:            "example_dag",
			MapIndex:         -1,
			RunId:            "manual__2026-06-10T00:00:00+00:00",
			TaskId:           "example_task",
			TryNumber:        2,
		})
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)

	client, err := edgeapi.NewClient(server.URL + "/")
	require.NoError(t, err)

	w := &worker{
		hostname: "test-worker",
		client:   client,
		queues:   []string{"default"},
		logger: slog.New(slog.NewJSONHandler(&logBuffer, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		})),
		maxConcurrency: 16,
	}
	w.freeConcurrency.Store(4)

	workload, slots, err := w.fetchJob(context.Background())
	require.NoError(t, err)
	require.NotNil(t, workload)
	require.Equal(t, int32(3), slots)
	require.Equal(t, secretToken, workload.Token)
	w.logger.Debug("Got allocation", "workload", jobInfo{
		ExecuteTaskWorkload: *workload,
		ConcurrencySlots:    slots,
	})

	logOutput := logBuffer.String()
	require.Contains(t, logOutput, "Fetched job")
	require.Contains(t, logOutput, "Got allocation")
	require.Contains(t, logOutput, "example_dag")
	require.Contains(t, logOutput, "example_task")
	require.NotContains(t, logOutput, secretToken)
	require.NotContains(t, logOutput, "\"token\"")
}

func TestActiveWorkloadsMapConcurrentAccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(server.Close)

	client, err := edgeapi.NewClient(server.URL + "/")
	require.NoError(t, err)

	discovery := shared.NewDiscovery(t.TempDir(), slog.New(slog.NewTextHandler(io.Discard, nil)))

	w := &worker{
		Discovery:       discovery,
		hostname:        "race-test-worker",
		client:          client,
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		maxConcurrency:  16,
		activeWorkloads: map[uuid.UUID]bundlev1.ExecuteTaskWorkload{},
	}
	w.freeConcurrency.Store(16)

	stopReaders := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stopReaders:
					return
				default:
					w.activeWorkloadsMu.Lock()
					_ = len(w.activeWorkloads)
					w.activeWorkloadsMu.Unlock()
				}
			}
		}()
	}

	var writers sync.WaitGroup
	for range 20 {
		writers.Add(1)
		go func() {
			defer writers.Done()
			workload := bundlev1.ExecuteTaskWorkload{}
			workload.TI.Id = uuid.New()
			_ = w.runWorkload(context.Background(), 1, workload)
		}()
	}
	writers.Wait()
	close(stopReaders)
	readers.Wait()
}
