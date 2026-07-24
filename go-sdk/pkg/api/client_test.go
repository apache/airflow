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

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestWithBearerTokenUsesRefreshedAPIToken(t *testing.T) {
	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := requestCount.Add(1)
		expectedToken := "execution-token"
		if requestNumber == 1 {
			expectedToken = "workload-token"
		}
		if got := r.Header.Get("Authorization"); got != fmt.Sprintf("Bearer %s", expectedToken) {
			http.Error(
				w,
				fmt.Sprintf("unexpected authorization header %q", got),
				http.StatusUnauthorized,
			)
			return
		}

		switch requestNumber {
		case 1:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Refreshed-API-Token", "execution-token")
			_, _ = w.Write([]byte("{}"))
		case 2:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewDefaultClient(server.URL)
	require.NoError(t, err)
	authenticatedClient, err := client.(*Client).WithBearerToken("workload-token")
	require.NoError(t, err)

	taskInstanceID := uuid.New()
	_, err = authenticatedClient.TaskInstances().
		Run(context.Background(), taskInstanceID, &TIEnterRunningPayload{
			Hostname:  "worker",
			Pid:       1,
			StartDate: time.Now(),
			State:     Running,
			Unixname:  "airflow",
		})
	require.NoError(t, err)
	err = authenticatedClient.TaskInstances().
		Heartbeat(context.Background(), taskInstanceID, &TIHeartbeatInfo{
			Hostname: "worker",
			Pid:      1,
		})
	require.NoError(t, err)
	require.EqualValues(t, 2, requestCount.Load())
}
