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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/airflow"
	"github.com/apache/airflow/go-sdk/sdk"
)

// unusedClient satisfies airflow.NewContext, which rejects a nil client.
// These handlers never call it.
type unusedClient struct{ sdk.Client }

func testContext(t *testing.T) airflow.Context {
	t.Helper()
	return airflow.NewContext(
		t.Context(), slog.Default(), unusedClient{},
		airflow.TaskInstance{}, airflow.DagRun{},
	)
}

func TestViaFlatArgs(t *testing.T) {
	got, err := ViaFlatArgs(testContext(t),
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
	_, err := ViaFlatArgs(testContext(t),
		"summary", 3, 2.5, true,
		[]string{"metrics", "hourly"},
		Config{},
		[]int{1, 1, 2, 3, 5, 8},
		nil,
	)
	assert.ErrorContains(t, err, "object XCom bound incorrectly")
}

func TestViaStructNoTags(t *testing.T) {
	got, err := ViaStructNoTags(testContext(t), ViaStructNoTagsInput{
		RegionCode: "eu-west-1",
		Threshold:  0.75,
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructNoTags should return a map summary, got %T", got)
	assert.Equal(t, "eu-west-1", summary["region_code"])
}

func TestViaStructNoTagsRejectsWrongBinding(t *testing.T) {
	_, err := ViaStructNoTags(testContext(t), ViaStructNoTagsInput{
		RegionCode: "wrong-region",
		Threshold:  0.75,
	})
	assert.ErrorContains(t, err, "struct fields bound incorrectly")
}

func TestViaStructArgTag(t *testing.T) {
	got, err := ViaStructArgTag(testContext(t), ViaStructArgTagInput{
		Region:    "eu-west-1",
		Threshold: 0.75,
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructArgTag should return a map summary, got %T", got)
	assert.Equal(t, "eu-west-1", summary["region"])
}

func TestViaStructArgTagRejectsWrongBinding(t *testing.T) {
	_, err := ViaStructArgTag(testContext(t), ViaStructArgTagInput{
		Region:    "wrong-region",
		Threshold: 0.75,
	})
	assert.ErrorContains(t, err, "struct fields bound incorrectly")
}

func TestViaStructUnmatchedArg(t *testing.T) {
	got, err := ViaStructUnmatchedArg(testContext(t), ViaStructUnmatchedArgInput{
		Region:  "eu-west-1",
		Missing: "",
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructUnmatchedArg should return a map summary, got %T", got)
	assert.Equal(t, true, summary["missing_was_empty"])
}

func TestViaStructUnmatchedArgRejectsNonZeroMissingField(t *testing.T) {
	_, err := ViaStructUnmatchedArg(testContext(t), ViaStructUnmatchedArgInput{
		Region:  "eu-west-1",
		Missing: "unexpected",
	})
	assert.ErrorContains(t, err, "expected the unmatched field to stay at its Go zero value")
}

func TestViaFlatMap(t *testing.T) {
	got, err := ViaFlatMap(testContext(t), FlatMapConfig{Region: "eu-west-1", Count: 3})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaFlatMap should return a map summary, got %T", got)
	assert.Equal(t, "eu-west-1", summary["region"])
	assert.Equal(t, 3, summary["count"])
}

func TestViaFlatMapRejectsWrongBinding(t *testing.T) {
	_, err := ViaFlatMap(testContext(t), FlatMapConfig{Region: "wrong-region", Count: 3})
	assert.ErrorContains(t, err, "whole-value map bound incorrectly")
}

func TestViaStructMap(t *testing.T) {
	got, err := ViaStructMap(testContext(t), StructMapInput{
		Payload: map[string]any{"region": "eu-west-1", "count": 3},
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaStructMap should return a map summary, got %T", got)
	assert.Equal(t, map[string]any{"region": "eu-west-1", "count": 3}, summary["payload"])
}

func TestViaStructMapRejectsWrongBinding(t *testing.T) {
	_, err := ViaStructMap(testContext(t), StructMapInput{
		Payload: map[string]any{"region": "wrong-region"},
	})
	assert.ErrorContains(t, err, "map field bound incorrectly")
}

func TestViaPlainMap(t *testing.T) {
	got, err := ViaPlainMap(testContext(t), map[string]string{
		"team": "data", "tier": "gold",
	})
	require.NoError(t, err)

	summary, ok := got.(map[string]any)
	require.True(t, ok, "ViaPlainMap should return a map summary, got %T", got)
	assert.Equal(t, "data", summary["team"])
	assert.Equal(t, "gold", summary["tier"])
}

func TestViaPlainMapRejectsWrongBinding(t *testing.T) {
	_, err := ViaPlainMap(testContext(t), map[string]string{"team": "wrong"})
	assert.ErrorContains(t, err, "plain map bound incorrectly")
}
