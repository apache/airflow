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

package execution

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
)

func TestSerializeValuePrimitives(t *testing.T) {
	assert.Nil(t, serializeValue(nil))
	assert.Equal(t, "hello", serializeValue("hello"))
	assert.Equal(t, true, serializeValue(true))
	assert.Equal(t, 42, serializeValue(42))
	assert.Equal(t, float64(3.14), serializeValue(3.14))
}

func TestSerializeValueDatetime(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 500000000, time.UTC)
	result := serializeValue(ts)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "datetime", m["__type"])
	epochSec := m["__var"].(float64)
	expected := float64(ts.Unix()) + 0.5
	assert.InDelta(t, expected, epochSec, 0.001)
}

func TestSerializeValueTimedelta(t *testing.T) {
	dur := 90 * time.Second
	result := serializeValue(dur)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "timedelta", m["__type"])
	assert.Equal(t, 90.0, m["__var"])
}

func TestSerializeValueMap(t *testing.T) {
	input := map[string]any{
		"key1": "val1",
		"key2": 42,
	}
	result := serializeValue(input)
	m, ok := result.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "dict", m["__type"])
	inner := m["__var"].(map[string]any)
	assert.Equal(t, "val1", inner["key1"])
	assert.Equal(t, 42, inner["key2"])
}

func TestSerializeValueSlice(t *testing.T) {
	input := []any{"a", 1, true}
	result := serializeValue(input)
	arr, ok := result.([]any)
	require.True(t, ok)
	assert.Len(t, arr, 3)
	assert.Equal(t, "a", arr[0])
}

func TestUnwrapTypeEncoding(t *testing.T) {
	wrapped := map[string]any{
		"__type": "datetime",
		"__var":  1705313400.5,
	}
	assert.Equal(t, 1705313400.5, unwrapTypeEncoding(wrapped))

	assert.Equal(t, "hello", unwrapTypeEncoding("hello"))
	assert.Equal(t, 42, unwrapTypeEncoding(42))
}

func TestSerializeTimetable(t *testing.T) {
	t.Run("nil schedule", func(t *testing.T) {
		result := serializeTimetable(nil)
		assert.Equal(t, "airflow.timetables.simple.NullTimetable", result["__type"])
	})

	t.Run("@once", func(t *testing.T) {
		s := "@once"
		result := serializeTimetable(&s)
		assert.Equal(t, "airflow.timetables.simple.OnceTimetable", result["__type"])
	})

	t.Run("@continuous", func(t *testing.T) {
		s := "@continuous"
		result := serializeTimetable(&s)
		assert.Equal(t, "airflow.timetables.simple.ContinuousTimetable", result["__type"])
	})

	t.Run("cron expression", func(t *testing.T) {
		s := "0 12 * * *"
		result := serializeTimetable(&s)
		assert.Equal(t, "airflow.timetables.trigger.CronTriggerTimetable", result["__type"])
		v := result["__var"].(map[string]any)
		assert.Equal(t, "0 12 * * *", v["expression"])
		assert.Equal(t, "UTC", v["timezone"])
		assert.Equal(t, 0.0, v["interval"])
		assert.Equal(t, false, v["run_immediately"])
	})
}

func TestSerializeTask(t *testing.T) {
	result := serializeTask("extract", "extract", "main", []string{"transform"})
	assert.Equal(t, "operator", result["__type"])
	data := result["__var"].(map[string]any)
	assert.Equal(t, "extract", data["task_id"])
	assert.Equal(t, "extract", data["task_type"])
	assert.Equal(t, "main", data["_task_module"])
	assert.Equal(t, "go", data["language"])
	assert.Equal(t, []string{"transform"}, data["downstream_task_ids"])
}

func TestSerializeTaskNoDownstream(t *testing.T) {
	result := serializeTask("load", "load", "main", nil)
	data := result["__var"].(map[string]any)
	_, hasDownstream := data["downstream_task_ids"]
	assert.False(t, hasDownstream)
}

func TestSerializeTaskGroup(t *testing.T) {
	result := serializeTaskGroup([]string{"t1", "t2"})
	assert.Nil(t, result["_group_id"])
	assert.Equal(t, true, result["prefix_group_id"])
	assert.Equal(t, "CornflowerBlue", result["ui_color"])

	children := result["children"].(map[string]any)
	assert.Equal(t, []any{"operator", "t1"}, children["t1"])
	assert.Equal(t, []any{"operator", "t2"}, children["t2"])
}

func TestSerializeParams(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		result := serializeParams(nil)
		assert.Equal(t, []any{}, result)
	})

	t.Run("with values", func(t *testing.T) {
		result := serializeParams(map[string]any{"key1": "default_val"})
		assert.Len(t, result, 1)
		pair := result[0].([]any)
		assert.Equal(t, "key1", pair[0])
		paramMap := pair[1].(map[string]any)
		assert.Equal(t, "airflow.sdk.definitions.param.Param", paramMap["__class"])
		assert.Equal(t, "default_val", paramMap["default"])
	})
}

func TestSerializeDagMinimal(t *testing.T) {
	info := bundlev1.DagInfo{DagID: "test_dag"}
	result := SerializeDag(info, "/path/to/bundle", ".")

	assert.Equal(t, "test_dag", result["dag_id"])
	assert.Equal(t, "/path/to/bundle", result["fileloc"])
	assert.Equal(t, ".", result["relative_fileloc"])
	assert.Equal(t, "UTC", result["timezone"])

	tt := result["timetable"].(map[string]any)
	assert.Equal(t, "airflow.timetables.simple.NullTimetable", tt["__type"])

	_, hasDesc := result["description"]
	assert.False(t, hasDesc)
	_, hasCatchup := result["catchup"]
	assert.False(t, hasCatchup)
}

func TestSerializeDagWithTasks(t *testing.T) {
	info := bundlev1.DagInfo{
		DagID: "etl",
		Tasks: []bundlev1.TaskInfo{
			{ID: "extract", TypeName: "extract", PkgPath: "main"},
			{ID: "load", TypeName: "load", PkgPath: "main"},
		},
	}
	result := SerializeDag(info, "/bundle/main.go", "main.go")

	tasks := result["tasks"].([]any)
	require.Len(t, tasks, 2)
	first := tasks[0].(map[string]any)
	v := first["__var"].(map[string]any)
	assert.Equal(t, "extract", v["task_id"])
	assert.Equal(t, "extract", v["task_type"])
	assert.Equal(t, "main", v["_task_module"])
	assert.Equal(t, "go", v["language"])

	tg := result["task_group"].(map[string]any)
	children := tg["children"].(map[string]any)
	assert.Contains(t, children, "extract")
	assert.Contains(t, children, "load")
}

func TestComputeRelativeFileloc(t *testing.T) {
	tests := []struct {
		fileloc    string
		bundlePath string
		want       string
	}{
		{"", "", ""},
		{"/a/b/c.go", "", "."},
		{"/bundles/my/dags.go", "/bundles/my", "dags.go"},
		{"/bundles/my/sub/dags.go", "/bundles/my", "sub/dags.go"},
	}
	for _, tt := range tests {
		result := computeRelativeFileloc(tt.fileloc, tt.bundlePath)
		assert.Equal(t, tt.want, result, "fileloc=%q bundlePath=%q", tt.fileloc, tt.bundlePath)
	}
}
