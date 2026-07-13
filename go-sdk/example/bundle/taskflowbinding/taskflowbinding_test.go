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

package taskflowbinding

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/sdk"
)

// Like example/bundle/main_test.go, this shows a task fn is unit-testable by
// passing the data parameters directly, exactly as the runtime binds them.
func TestCombine(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	got, err := Combine(ctx, slog.Default(),
		"summary", 3, 2.5, true,
		[]string{"metrics", "hourly"},
		Config{Environment: "production", Region: "eu-west-1", Debug: true},
		[]int{1, 1, 2, 3, 5, 8},
		nil,
	)
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "Combine should return a map summary, got %T", got)
	assert.Equal(t, 20, summary["sum"])
	assert.Equal(t, true, summary["note_was_null"])
}

func TestCombineRejectsWrongBinding(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	_, err := Combine(ctx, slog.Default(),
		"summary", 3, 2.5, true,
		[]string{"metrics", "hourly"},
		Config{},
		[]int{1, 1, 2, 3, 5, 8},
		nil,
	)
	assert.ErrorContains(t, err, "object XCom bound incorrectly")
}
