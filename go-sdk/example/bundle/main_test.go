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

package main

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "github.com/apache/airflow/go-sdk/bundle/bundlev1"
	"github.com/apache/airflow/go-sdk/sdk"
)

// This file serves as an example of how you could write unit tests against your own Go Tasks.
// An example of how to write a test for a Task function!

type mockVars struct{}

// GetVariable implements sdk.VariableClient.
func (m *mockVars) GetVariable(ctx context.Context, key string) (string, error) {
	switch key {
	case "my_variable":
		return "value1", nil
	default:
		return "", sdk.VariableNotFound
	}
}

// UnmarshalJSONVariable implements sdk.VariableClient.
func (m *mockVars) UnmarshalJSONVariable(ctx context.Context, key string, pointer any) error {
	panic("unimplemented")
}

var _ sdk.VariableClient = (*mockVars)(nil)

func TestRegisterDags(t *testing.T) {
	registry := v1.New()
	require.NoError(t, (&myBundle{}).RegisterDags(registry))

	dags := registry.(v1.EnumerableBundle).OrderedDags()
	require.Len(t, dags, 4)

	simpleDag := dags[0]
	assert.Equal(t, "simple_dag", simpleDag.DagID)
	assert.Equal(t, v1.DagSpec{}, simpleDag.Spec)
	require.Len(t, simpleDag.Tasks, 3)
	assert.Equal(t, "extract", simpleDag.Tasks[0].ID)
	assert.Empty(t, simpleDag.Tasks[0].Downstream)
	assert.Equal(t, "transform", simpleDag.Tasks[1].ID)
	assert.Empty(t, simpleDag.Tasks[1].Downstream)
	assert.Equal(t, "load", simpleDag.Tasks[2].ID)
	assert.Empty(t, simpleDag.Tasks[2].Downstream)

	nativeDag := dags[1]
	assert.Equal(t, "native_dag", nativeDag.DagID)
	assert.Equal(t, "@daily", nativeDag.Spec.Schedule)
	assert.Equal(t, []string{"example", "go-sdk"}, nativeDag.Spec.Tags)
	require.Len(t, nativeDag.Tasks, 3)
	assert.Equal(t, []string{"nativeTransform"}, nativeDag.Tasks[0].Downstream)
	assert.Equal(t, []string{"nativeLoad"}, nativeDag.Tasks[1].Downstream)
	assert.Empty(t, nativeDag.Tasks[2].Downstream)
}

func Test_transform(t *testing.T) {
	log := slog.Default()
	// This is not the best test, but it is a good proof of concept -- you can just call the function.
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	err := transform(ctx, &mockVars{}, log, "uk", map[string]any{"go_version": "go1.24"})
	assert.NoError(t, err)
}
