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
	result := serializeTask(
		bundlev1.TaskInfo{ID: "extract", TypeName: "extract", PkgPath: "main"},
		[]string{"transform"},
	)
	assert.Equal(t, "operator", result["__type"])
	data := result["__var"].(map[string]any)
	assert.Equal(t, "extract", data["task_id"])
	assert.Equal(t, "extract", data["task_type"])
	assert.Equal(t, "main", data["_task_module"])
	assert.Equal(t, "go", data["language"])
	assert.Equal(t, []string{"transform"}, data["downstream_task_ids"])
	_, hasQueue := data["queue"]
	assert.False(t, hasQueue, "queue should be omitted when unset")
}

func TestSerializeTaskNoDownstream(t *testing.T) {
	result := serializeTask(
		bundlev1.TaskInfo{ID: "load", TypeName: "load", PkgPath: "main"},
		nil,
	)
	data := result["__var"].(map[string]any)
	_, hasDownstream := data["downstream_task_ids"]
	assert.False(t, hasDownstream)
}

func TestSerializeTaskCustomQueue(t *testing.T) {
	result := serializeTask(
		bundlev1.TaskInfo{
			ID: "extract", TypeName: "extract", PkgPath: "main",
			Spec: bundlev1.TaskSpec{Queue: "high_mem"},
		},
		nil,
	)
	data := result["__var"].(map[string]any)
	assert.Equal(t, "high_mem", data["queue"])
}

func TestSerializeTaskDefaultQueueOmitted(t *testing.T) {
	result := serializeTask(
		bundlev1.TaskInfo{
			ID: "extract", TypeName: "extract", PkgPath: "main",
			Spec: bundlev1.TaskSpec{Queue: "default"},
		},
		nil,
	)
	data := result["__var"].(map[string]any)
	_, hasQueue := data["queue"]
	assert.False(t, hasQueue, "queue=\"default\" matches the schema default and should be omitted")
}

func TestApplyTaskSpec_EmitsAndOmits(t *testing.T) {
	spec := bundlev1.TaskSpec{
		Queue:                   "gpu",
		Pool:                    "gpu_pool",
		PoolSlots:               4,
		Retries:                 3,
		RetryDelay:              60 * time.Second,
		MaxRetryDelay:           10 * time.Minute,
		RetryExponentialBackoff: 2.0,
		PriorityWeight:          5,
		WeightRule:              "upstream",
		TriggerRule:             "all_done",
		Owner:                   "data-eng",
		ExecutionTimeout:        45 * time.Second,
		Executor:                "KubernetesExecutor",
		DependsOnPast:           true,
		WaitForDownstream:       true,
		DoXComPush:              bundlev1.Bool(false),
		EmailOnFailure:          bundlev1.Bool(false),
		EmailOnRetry:            bundlev1.Bool(false),
		DocMD:                   "## task",
		MapIndexTemplate:        "{{ task.task_id }}",
		MaxActiveTisPerDag:      2,
		MaxActiveTisPerDagrun:   1,
	}
	data := map[string]any{}
	applyTaskSpec(data, spec)

	assert.Equal(t, "gpu", data["queue"])
	assert.Equal(t, "gpu_pool", data["pool"])
	assert.Equal(t, 4, data["pool_slots"])
	assert.Equal(t, 3, data["retries"])
	assert.Equal(t, 60.0, data["retry_delay"])
	assert.Equal(t, 600.0, data["max_retry_delay"])
	assert.Equal(t, 2.0, data["retry_exponential_backoff"])
	assert.Equal(t, 5, data["priority_weight"])
	assert.Equal(t, "upstream", data["weight_rule"])
	assert.Equal(t, "all_done", data["trigger_rule"])
	assert.Equal(t, "data-eng", data["owner"])
	assert.Equal(t, 45.0, data["execution_timeout"])
	assert.Equal(t, "KubernetesExecutor", data["executor"])
	assert.Equal(t, true, data["depends_on_past"])
	assert.Equal(t, true, data["wait_for_downstream"])
	assert.Equal(t, false, data["do_xcom_push"])
	assert.Equal(t, false, data["email_on_failure"])
	assert.Equal(t, false, data["email_on_retry"])
	assert.Equal(t, "## task", data["doc_md"])
	assert.Equal(t, "{{ task.task_id }}", data["map_index_template"])
	assert.Equal(t, 2, data["max_active_tis_per_dag"])
	assert.Equal(t, 1, data["max_active_tis_per_dagrun"])
}

func TestApplyTaskSpec_OmitsSchemaDefaults(t *testing.T) {
	// Values equal to schema defaults must be dropped.
	spec := bundlev1.TaskSpec{
		Queue:          "default",
		Pool:           "default_pool",
		PoolSlots:      1,
		Retries:        0,
		RetryDelay:     300 * time.Second,
		PriorityWeight: 1,
		WeightRule:     "downstream",
		TriggerRule:    "all_success",
		Owner:          "airflow",
		DoXComPush:     bundlev1.Bool(true),
		EmailOnFailure: bundlev1.Bool(true),
		EmailOnRetry:   bundlev1.Bool(true),
	}
	data := map[string]any{}
	applyTaskSpec(data, spec)
	assert.Empty(t, data, "all fields equal schema defaults; nothing should be emitted")
}

func TestApplyTaskSpec_EmptySpecNoOp(t *testing.T) {
	data := map[string]any{}
	applyTaskSpec(data, bundlev1.TaskSpec{})
	assert.Empty(t, data)
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
			{
				ID: "load", TypeName: "load", PkgPath: "main",
				Spec: bundlev1.TaskSpec{Queue: "high_mem"},
			},
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
	_, hasQueue := v["queue"]
	assert.False(t, hasQueue, "extract has no queue set; field should be omitted")

	second := tasks[1].(map[string]any)["__var"].(map[string]any)
	assert.Equal(t, "high_mem", second["queue"])

	tg := result["task_group"].(map[string]any)
	children := tg["children"].(map[string]any)
	assert.Contains(t, children, "extract")
	assert.Contains(t, children, "load")
}

func TestSerializeDagWithSpec(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	info := bundlev1.DagInfo{
		DagID: "etl",
		Spec: bundlev1.DagSpec{
			Schedule:                    "@daily",
			Description:                 "Extract, transform, load",
			StartDate:                   start,
			Tags:                        []string{"prod", "etl"},
			DagDisplayName:              "ETL Pipeline",
			DocMD:                       "## ETL",
			MaxActiveTasks:              32,
			MaxActiveRuns:               4,
			MaxConsecutiveFailedDagRuns: 3,
			DagrunTimeout:               2 * time.Hour,
			Catchup:                     true,
			FailFast:                    true,
			RenderTemplateAsNativeObj:   true,
			DisableBundleVersioning:     true,
			IsPausedUponCreation:        bundlev1.Bool(true),
		},
	}
	result := SerializeDag(info, "/bundle/main.go", "main.go")

	tt := result["timetable"].(map[string]any)
	assert.Equal(t, "airflow.timetables.trigger.CronTriggerTimetable", tt["__type"])
	v := tt["__var"].(map[string]any)
	assert.Equal(t, "@daily", v["expression"])

	assert.Equal(t, "Extract, transform, load", result["description"])
	assert.Equal(t, []any{"prod", "etl"}, result["tags"])
	assert.Equal(t, "ETL Pipeline", result["dag_display_name"])
	assert.Equal(t, "## ETL", result["doc_md"])
	assert.Equal(t, 32, result["max_active_tasks"])
	assert.Equal(t, 4, result["max_active_runs"])
	assert.Equal(t, 3, result["max_consecutive_failed_dag_runs"])
	assert.Equal(t, (2 * time.Hour).Seconds(), result["dagrun_timeout"])
	assert.Equal(t, true, result["catchup"])
	assert.Equal(t, true, result["fail_fast"])
	assert.Equal(t, true, result["render_template_as_native_obj"])
	assert.Equal(t, true, result["disable_bundle_versioning"])
	assert.Equal(t, true, result["is_paused_upon_creation"])

	// start_date is a raw epoch number, not the type-wrapped form.
	startDate := result["start_date"].(float64)
	assert.InDelta(t, float64(start.Unix()), startDate, 0.001)
}

func TestApplyDagSpec_OmitsSchemaDefaults(t *testing.T) {
	spec := bundlev1.DagSpec{
		MaxActiveTasks: 16,
		MaxActiveRuns:  16,
	}
	data := map[string]any{}
	applyDagSpec(data, spec)
	assert.Empty(t, data, "values equal to schema defaults must be omitted")
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
