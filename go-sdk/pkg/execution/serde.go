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

// serializeValue matches Python's type/var encoding.
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

// unwrapTypeEncoding strips type/var metadata.
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
// TODO: honor scheduler interval flags once the supervisor sends them.
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

// serializeTask builds a serialized task.
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
		// Python always emits this; Go tasks have no template fields.
		"template_fields": []any{},
	}
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

// applyDagSpec writes serialized Dag fields.
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

// serializeParams builds serialized Dag params.
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

// SerializeDag converts DagInfo to DagSerialization v3.
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

// computeRelativeFileloc returns a path relative to the bundle.
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
