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
//
// TODO: respect [scheduler] create_cron_data_intervals (and, once timedelta
// schedules are supported, create_delta_data_intervals). Python's
// _create_timetable selects CronDataIntervalTimetable when
// create_cron_data_intervals is True and CronTriggerTimetable when False; we
// hardcode CronTriggerTimetable, which matches only the default (False). The
// Go bundle binary cannot read airflow.cfg, so the supervisor (Python
// ExecutableCoordinator) must send these scheduler flags to the lang-SDK over
// the coordinator protocol (e.g. on DagFileParseRequest) before we can honor
// non-default deployments. Until that channel exists this stays default-only.
// Tracked at https://github.com/apache/airflow/issues/67938
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

// serializeTask converts a task to the Airflow serialization format. The
// downstream_task_ids slice is read from info.Downstream (populated by the
// registry from each task's `depends` argument) and sorted for stable JSON.
func serializeTask(info bundlev1.TaskInfo) map[string]any {
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
		// Python's operator serializer always emits template_fields (its
		// list value never matches the tuple default it is compared against),
		// so it is unconditional here too. Go tasks have no template fields.
		"template_fields": []any{},
	}
	// TaskSpec.SchemaFields (generated from schema.json) returns only the
	// fields that are set and differ from their schema default, so this
	// mirrors Python BaseSerialization's "omit hard-coded default" behavior.
	// serializeValue converts time.Duration / time.Time to their wire floats;
	// unwrapTypeEncoding strips the __type wrapper because operator fields are
	// stored unwrapped.
	for key, value := range info.Spec.SchemaFields() {
		data[key] = unwrapTypeEncoding(serializeValue(value))
	}
	if len(info.Downstream) > 0 {
		sorted := make([]string, len(info.Downstream))
		copy(sorted, info.Downstream)
		sort.Strings(sorted)
		data["downstream_task_ids"] = sorted
	}
	return map[string]any{
		"__type": "operator",
		"__var":  data,
	}
}

// applyDagSpec writes DAG-level fields onto data. The emit policy — which
// keys are omitted when unset, always emitted, or fall back to a [core]
// config default — lives in the generated DagSpec.SchemaFields; this only
// converts the returned values to the wire encoding, as serializeTask does
// for TaskSpec.
func applyDagSpec(data map[string]any, s bundlev1.DagSpec) {
	for key, value := range s.SchemaFields() {
		data[key] = unwrapTypeEncoding(serializeValue(value))
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
// format. Required fields are always present; spec-driven fields are emitted
// per the generated DagSpec.SchemaFields policy (some always, some only when
// set).
func SerializeDag(info bundlev1.DagInfo, fileloc, relativeFileloc string) map[string]any {
	taskIDs := make([]string, len(info.Tasks))
	tasks := make([]any, len(info.Tasks))
	for i, t := range info.Tasks {
		taskIDs[i] = t.ID
		tasks[i] = serializeTask(t)
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
