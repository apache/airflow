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

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/sdk"
)

// This file serves as an example of how you could write unit tests against your own Go Tasks.
// An example of how to write a test for a Task function!

// mockVars embeds sdk.Client so it satisfies the whole interface while only defining
// the method this test expects. Calling any other method panics on the nil embedded client.
type mockVars struct{ sdk.Client }

func (m *mockVars) GetVariable(ctx context.Context, key string) (string, error) {
	switch key {
	case "my_variable":
		return "value1", nil
	default:
		return "", sdk.VariableNotFound
	}
}

func Test_transform(t *testing.T) {
	// This is not the best test, but it is a good proof of concept -- you can just call the function.
	actx := airflow.NewContext(
		context.Background(), slog.Default(), &mockVars{},
		airflow.TaskInstance{}, airflow.DagRun{},
	)
	err := transform(actx, "uk", map[string]any{"go_version": "go1.24"})
	assert.NoError(t, err)
}
