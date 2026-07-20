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
func TestViaFlatArgs(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	got, err := ViaFlatArgs(ctx, slog.Default(),
		"summary", 3, 2.5, true,
		[]string{"metrics", "hourly"},
		Config{Environment: "production", Region: "eu-west-1", Debug: true},
		[]int{1, 1, 2, 3, 5, 8},
		nil,
	)
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaFlatArgs should return a map summary, got %T", got)
	assert.Equal(t, 20, summary["sum"])
	assert.Equal(t, true, summary["note_was_null"])
}

func TestViaFlatArgsRejectsWrongBinding(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	_, err := ViaFlatArgs(ctx, slog.Default(),
		"summary", 3, 2.5, true,
		[]string{"metrics", "hourly"},
		Config{},
		[]int{1, 1, 2, 3, 5, 8},
		nil,
	)
	assert.ErrorContains(t, err, "object XCom bound incorrectly")
}

func TestViaStructNoTags(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	got, err := ViaStructNoTags(ctx, slog.Default(), ViaStructNoTagsInput{
		RegionCode: "eu-west-1",
		Threshold:  0.75,
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructNoTags should return a map summary, got %T", got)
	assert.Equal(t, "eu-west-1", summary["region_code"])
}

func TestViaStructNoTagsRejectsWrongBinding(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	_, err := ViaStructNoTags(ctx, slog.Default(), ViaStructNoTagsInput{
		RegionCode: "wrong-region",
		Threshold:  0.75,
	})
	assert.ErrorContains(t, err, "TaskInput fields bound incorrectly")
}

func TestViaStructArgTag(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	got, err := ViaStructArgTag(ctx, slog.Default(), ViaStructArgTagInput{
		Region:    "eu-west-1",
		Threshold: 0.75,
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructArgTag should return a map summary, got %T", got)
	assert.Equal(t, "eu-west-1", summary["region"])
}

func TestViaStructArgTagRejectsWrongBinding(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	_, err := ViaStructArgTag(ctx, slog.Default(), ViaStructArgTagInput{
		Region:    "wrong-region",
		Threshold: 0.75,
	})
	assert.ErrorContains(t, err, "TaskInput fields bound incorrectly")
}

func TestViaStructXComTag(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	got, err := ViaStructXComTag(ctx, slog.Default(), ViaStructXComTagInput{
		Threshold: 0.75,
		Config:    Config{Environment: "production", Region: "eu-west-1", Debug: true},
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructXComTag should return a map summary, got %T", got)
	assert.Equal(t, "production", summary["environment"])
}

func TestViaStructXComTagRejectsWrongBinding(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	_, err := ViaStructXComTag(ctx, slog.Default(), ViaStructXComTagInput{
		Threshold: 0.75,
		Config:    Config{},
	})
	assert.ErrorContains(t, err, "ad hoc xcom field bound incorrectly")
}

func TestViaStructUnmatchedArg(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	// Missing is left at its Go zero value, exactly as binding.Resolve leaves an
	// unmatched TaskInput field -- this task fn is unit-testable independent of
	// the binding package precisely because it declares that expectation itself.
	got, err := ViaStructUnmatchedArg(ctx, slog.Default(), ViaStructUnmatchedArgInput{
		Region:  "eu-west-1",
		Missing: "",
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructUnmatchedArg should return a map summary, got %T", got)
	assert.Equal(t, true, summary["missing_was_empty"])
}

func TestViaStructUnmatchedArgRejectsNonZeroMissingField(t *testing.T) {
	ctx := sdk.NewTIRunContext(context.Background(), sdk.TaskInstance{}, sdk.DagRun{})
	_, err := ViaStructUnmatchedArg(ctx, slog.Default(), ViaStructUnmatchedArgInput{
		Region:  "eu-west-1",
		Missing: "unexpected",
	})
	assert.ErrorContains(t, err, "expected the unmatched field to stay at its Go zero value")
}
