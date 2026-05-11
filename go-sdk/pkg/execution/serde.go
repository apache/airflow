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
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/apache/airflow/go-sdk/bundle/bundlev1"
)

// serializeValue recursively serializes a value with Airflow's type/var encoding.
// This matches Python's BaseSerialization.serialize() output:
//   - primitives (string, bool, int, float) pass through unchanged
//   - time.Time -> {"__type": "datetime", "__var": epoch_seconds_float}
//   - time.Duration -> {"__type": "timedelta", "__var": total_seconds_float}
//   - map[string]any -> {"__type": "dict", "__var": {k: serialize(v), ...}}
//   - []any -> direct array with each element serialized
func serializeValue(value any) any {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case string, bool:
		return v
	case int:
		return v
	case int8:
		return int(v)
	case int16:
		return int(v)
	case int32:
		return int(v)
	case int64:
		return v
	case float32:
		return float64(v)
	case float64:
		return v
	case time.Time:
		epochSec := float64(v.Unix()) + float64(v.Nanosecond())/1e9
		return map[string]any{
			"__type": "datetime",
			"__var":  epochSec,
		}
	case time.Duration:
		return map[string]any{
			"__type": "timedelta",
			"__var":  v.Seconds(),
		}
	case map[string]any:
		serialized := make(map[string]any, len(v))
		for k, val := range v {
			serialized[k] = serializeValue(val)
		}
		return map[string]any{
			"__type": "dict",
			"__var":  serialized,
		}
	case []string:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = serializeValue(item)
		}
		return result
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = serializeValue(item)
		}
		return result
	default:
		// Use reflection to handle typed maps and slices that don't match
		// the concrete types above (e.g., map[string]map[string][]string).
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map:
			serialized := make(map[string]any, rv.Len())
			for _, key := range rv.MapKeys() {
				serialized[fmt.Sprint(key.Interface())] = serializeValue(rv.MapIndex(key).Interface())
			}
			return map[string]any{
				"__type": "dict",
				"__var":  serialized,
			}
		case reflect.Slice, reflect.Array:
			result := make([]any, rv.Len())
			for i := range result {
				result[i] = serializeValue(rv.Index(i).Interface())
			}
			return result
		default:
			return v
		}
	}
}

// unwrapTypeEncoding extracts the "__var" part from a type-encoded value.
// In Python's serialize_to_json, non-decorated fields are serialized then unwrapped.
func unwrapTypeEncoding(value any) any {
	m, ok := value.(map[string]any)
	if !ok {
		return value
	}
	if _, hasType := m["__type"]; !hasType {
		return value
	}
	if v, hasVar := m["__var"]; hasVar {
		return v
	}
	return value
}

// serializeTimetable converts a schedule string to the Airflow timetable format.
func serializeTimetable(schedule *string) map[string]any {
	if schedule == nil {
		return map[string]any{
			"__type": "airflow.timetables.simple.NullTimetable",
			"__var":  map[string]any{},
		}
	}
	switch *schedule {
	case "@once":
		return map[string]any{
			"__type": "airflow.timetables.simple.OnceTimetable",
			"__var":  map[string]any{},
		}
	case "@continuous":
		return map[string]any{
			"__type": "airflow.timetables.simple.ContinuousTimetable",
			"__var":  map[string]any{},
		}
	default:
		return map[string]any{
			"__type": "airflow.timetables.trigger.CronTriggerTimetable",
			"__var": map[string]any{
				"expression":      *schedule,
				"timezone":        "UTC",
				"interval":        0.0,
				"run_immediately": false,
			},
		}
	}
}

// serializeTask converts a task to the Airflow serialization format.
func serializeTask(info bundlev1.TaskInfo, downstream []string) map[string]any {
	typeName := info.TypeName
	if typeName == "" {
		typeName = info.ID
	}
	pkgPath := info.PkgPath
	if pkgPath == "" {
		pkgPath = "main"
	}
	data := map[string]any{
		"task_id":      info.ID,
		"task_type":    typeName,
		"_task_module": pkgPath,
		"language":     "go",
	}
	applyTaskSpec(data, info.Spec)
	if len(downstream) > 0 {
		sorted := make([]string, len(downstream))
		copy(sorted, downstream)
		sort.Strings(sorted)
		data["downstream_task_ids"] = sorted
	}
	return map[string]any{
		"__type": "operator",
		"__var":  data,
	}
}

// applyTaskSpec mirrors Python BaseSerialization's "omit hard-coded default"
// behavior: each TaskSpec field is written into data only when it differs
// from the schema default declared in
// airflow-core/src/airflow/serialization/schema.json. A zero-valued field is
// always considered "unset" and is skipped.
func applyTaskSpec(data map[string]any, s bundlev1.TaskSpec) {
	if s.Queue != "" && s.Queue != "default" {
		data["queue"] = s.Queue
	}
	if s.Pool != "" && s.Pool != "default_pool" {
		data["pool"] = s.Pool
	}
	if s.PoolSlots != 0 && s.PoolSlots != 1 {
		data["pool_slots"] = s.PoolSlots
	}
	if s.Retries != 0 {
		data["retries"] = s.Retries
	}
	if s.RetryDelay != 0 && s.RetryDelay != 300*time.Second {
		data["retry_delay"] = unwrapTypeEncoding(serializeValue(s.RetryDelay))
	}
	if s.MaxRetryDelay != 0 {
		data["max_retry_delay"] = unwrapTypeEncoding(serializeValue(s.MaxRetryDelay))
	}
	if s.RetryExponentialBackoff != 0 {
		data["retry_exponential_backoff"] = s.RetryExponentialBackoff
	}
	if s.PriorityWeight != 0 && s.PriorityWeight != 1 {
		data["priority_weight"] = s.PriorityWeight
	}
	if s.WeightRule != "" && s.WeightRule != "downstream" {
		data["weight_rule"] = s.WeightRule
	}
	if s.TriggerRule != "" && s.TriggerRule != "all_success" {
		data["trigger_rule"] = s.TriggerRule
	}
	if s.Owner != "" && s.Owner != "airflow" {
		data["owner"] = s.Owner
	}
	if s.ExecutionTimeout != 0 {
		data["execution_timeout"] = unwrapTypeEncoding(serializeValue(s.ExecutionTimeout))
	}
	if s.Executor != "" {
		data["executor"] = s.Executor
	}
	if !s.StartDate.IsZero() {
		data["start_date"] = unwrapTypeEncoding(serializeValue(s.StartDate))
	}
	if !s.EndDate.IsZero() {
		data["end_date"] = unwrapTypeEncoding(serializeValue(s.EndDate))
	}
	if s.DependsOnPast {
		data["depends_on_past"] = true
	}
	if s.WaitForDownstream {
		data["wait_for_downstream"] = true
	}
	// do_xcom_push / email_on_failure / email_on_retry default to true; only
	// emit when an explicit false overrides the default.
	if s.DoXComPush != nil && !*s.DoXComPush {
		data["do_xcom_push"] = false
	}
	if s.EmailOnFailure != nil && !*s.EmailOnFailure {
		data["email_on_failure"] = false
	}
	if s.EmailOnRetry != nil && !*s.EmailOnRetry {
		data["email_on_retry"] = false
	}
	if s.DocMD != "" {
		data["doc_md"] = s.DocMD
	}
	if s.MapIndexTemplate != "" {
		data["map_index_template"] = s.MapIndexTemplate
	}
	if s.MaxActiveTisPerDag != 0 {
		data["max_active_tis_per_dag"] = s.MaxActiveTisPerDag
	}
	if s.MaxActiveTisPerDagrun != 0 {
		data["max_active_tis_per_dagrun"] = s.MaxActiveTisPerDagrun
	}
}

// applyDagSpec writes optional DAG-level fields onto data, omitting any
// field equal to its schema default. See applyTaskSpec for the convention.
func applyDagSpec(data map[string]any, s bundlev1.DagSpec) {
	if s.Description != "" {
		data["description"] = s.Description
	}
	if !s.StartDate.IsZero() {
		data["start_date"] = unwrapTypeEncoding(serializeValue(s.StartDate))
	}
	if !s.EndDate.IsZero() {
		data["end_date"] = unwrapTypeEncoding(serializeValue(s.EndDate))
	}
	if len(s.Tags) > 0 {
		tags := make([]any, len(s.Tags))
		for i, t := range s.Tags {
			tags[i] = t
		}
		data["tags"] = tags
	}
	if s.DagDisplayName != "" {
		data["dag_display_name"] = s.DagDisplayName
	}
	if s.DocMD != "" {
		data["doc_md"] = s.DocMD
	}
	if s.MaxActiveTasks != 0 && s.MaxActiveTasks != 16 {
		data["max_active_tasks"] = s.MaxActiveTasks
	}
	if s.MaxActiveRuns != 0 && s.MaxActiveRuns != 16 {
		data["max_active_runs"] = s.MaxActiveRuns
	}
	if s.MaxConsecutiveFailedDagRuns != 0 {
		data["max_consecutive_failed_dag_runs"] = s.MaxConsecutiveFailedDagRuns
	}
	if s.DagrunTimeout != 0 {
		data["dagrun_timeout"] = unwrapTypeEncoding(serializeValue(s.DagrunTimeout))
	}
	if s.Catchup {
		data["catchup"] = true
	}
	if s.FailFast {
		data["fail_fast"] = true
	}
	if s.RenderTemplateAsNativeObj {
		data["render_template_as_native_obj"] = true
	}
	if s.DisableBundleVersioning {
		data["disable_bundle_versioning"] = true
	}
	if s.IsPausedUponCreation != nil {
		data["is_paused_upon_creation"] = *s.IsPausedUponCreation
	}
}

// serializeTaskGroup creates a flat root task group containing all task IDs.
func serializeTaskGroup(taskIDs []string) map[string]any {
	children := make(map[string]any, len(taskIDs))
	for _, id := range taskIDs {
		children[id] = []any{"operator", id}
	}
	return map[string]any{
		"_group_id":            nil,
		"group_display_name":   "",
		"prefix_group_id":      true,
		"tooltip":              "",
		"ui_color":             "CornflowerBlue",
		"ui_fgcolor":           "#000",
		"children":             children,
		"upstream_group_ids":   []any{},
		"downstream_group_ids": []any{},
		"upstream_task_ids":    []any{},
		"downstream_task_ids":  []any{},
	}
}

// serializeParams converts DAG params to Airflow's serialization format.
func serializeParams(params map[string]any) []any {
	if len(params) == 0 {
		return []any{}
	}
	result := make([]any, 0, len(params))
	for k, v := range params {
		result = append(result, []any{
			k,
			map[string]any{
				"__class":     "airflow.sdk.definitions.param.Param",
				"default":     serializeValue(v),
				"description": nil,
				"schema":      serializeValue(map[string]any{}),
				"source":      nil,
			},
		})
	}
	return result
}

// SerializeDag converts a bundlev1.DagInfo to Airflow DagSerialization v3
// format. Required fields are always present; optional fields from
// info.Spec are emitted only when they differ from their schema default
// (see applyDagSpec).
func SerializeDag(info bundlev1.DagInfo, fileloc, relativeFileloc string) map[string]any {
	taskIDs := make([]string, len(info.Tasks))
	tasks := make([]any, len(info.Tasks))
	for i, t := range info.Tasks {
		taskIDs[i] = t.ID
		tasks[i] = serializeTask(t, nil)
	}

	var schedule *string
	if info.Spec.Schedule != "" {
		s := info.Spec.Schedule
		schedule = &s
	}

	result := map[string]any{
		// Required fields (always present)
		"dag_id":            info.DagID,
		"fileloc":           fileloc,
		"relative_fileloc":  relativeFileloc,
		"timezone":          "UTC",
		"timetable":         serializeTimetable(schedule),
		"tasks":             tasks,
		"dag_dependencies":  []any{},
		"task_group":        serializeTaskGroup(taskIDs),
		"edge_info":         map[string]any{},
		"params":            serializeParams(nil),
		"deadline":          nil,
		"allowed_run_types": nil,
	}
	applyDagSpec(result, info.Spec)
	return result
}

// computeRelativeFileloc computes the relative file location from the bundle path.
func computeRelativeFileloc(fileloc, bundlePath string) string {
	if fileloc == "" {
		return ""
	}
	if bundlePath == "" {
		return "."
	}
	rel, err := filepath.Rel(bundlePath, fileloc)
	if err != nil {
		return "."
	}
	if rel == "" {
		return "."
	}
	return rel
}
