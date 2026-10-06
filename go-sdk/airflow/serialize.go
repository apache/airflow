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

package airflow

import (
	"cmp"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// A serialized Dag is the Dag JSON that Airflow stores for a Dag and that its scheduler reads.
// Airflow core owns the format. airflow-core/src/airflow/serialization/schema.json describes it,
// and Python's DagSerialization writes it for a Dag authored in Python. The serializer in this file
// does not write the same bytes as Python. Python picks the fields to leave out with a
// client_defaults table that a bundle never gets, so Python writes some fields at their schema
// default, such as retry_delay, that this serializer leaves out. Airflow reads a missing field as
// its schema default, so it loads the same Dag from either serialization.
// scripts/ci/lang_sdk_serialization/compare.py checks that for the Dags in test_dags.yaml in the
// same directory.

// dagSerializationVersion is the __version of a serialized Dag. It is the SERIALIZER_VERSION that
// Airflow core's DagSerialization checks when it loads a serialized Dag. The supervisor schema has
// versions named by date, which Airflow negotiates with each runtime, but the Dag JSON that a
// bundle returns has only this number.
const dagSerializationVersion = 3

// A task that runs a Go function gets this task_type and _task_module, where a task of a Python Dag
// names the class and the module of its operator. Airflow never imports _task_module, so the two
// can name the coordinator that runs the task instead, as the TypeScript SDK does for its tasks.
// Nothing in Airflow reads language. It tells a reader of the serialized Dag which SDK wrote the
// task.
const (
	goTaskType   = "GoOperator"
	goTaskModule = "airflow.sdk.coordinators.executable"
	goLanguage   = "go"
)

// Python's serializer writes these values for a TriggerDagRunOperator. The operator class is in
// providers/standard/src/airflow/providers/standard/operators/trigger_dagrun.py.
const (
	triggerDagRunTaskType   = "TriggerDagRunOperator"
	triggerDagRunTaskModule = "airflow.providers.standard.operators.trigger_dagrun"
	triggerDagRunUIColor    = "#ffefeb"
)

var triggerDagRunTemplateFields = []string{
	"trigger_dag_id",
	"trigger_run_id",
	"logical_date",
	"conf",
	"wait_for_completion",
	"skip_when_already_exists",
}

// The ui_color and ui_fgcolor that Python's TaskGroup uses when the Dag author sets neither.
const (
	defaultGroupUIColor   = "CornflowerBlue"
	defaultGroupUIFgColor = "#000"
)

// A Schedule maps to one of these timetables. Python's DAG picks the same ones for a schedule
// string while [scheduler] create_cron_data_intervals is false, which is its default. When the
// option is true, Python picks CronDataIntervalTimetable for a cron expression instead. A bundle
// cannot read the option, so a Go Dag always gets CronTriggerTimetable
// (https://github.com/apache/airflow/issues/67938).
const (
	nullTimetable        = "airflow.timetables.simple.NullTimetable"
	onceTimetable        = "airflow.timetables.simple.OnceTimetable"
	continuousTimetable  = "airflow.timetables.simple.ContinuousTimetable"
	cronTriggerTimetable = "airflow.timetables.trigger.CronTriggerTimetable"
)

// cronPresets maps each cron preset to the expression that Python records for it. It holds the same
// entries as cron_presets in airflow-core/src/airflow/utils/dates.py.
var cronPresets = map[string]string{
	"@hourly":    "0 * * * *",
	"@daily":     "0 0 * * *",
	"@weekly":    "0 0 * * 0",
	"@monthly":   "0 0 1 * *",
	"@quarterly": "0 0 1 */3 *",
	"@yearly":    "0 0 1 1 *",
}

// croniterAliases are the presets that croniter, which reads a cron expression for Airflow, accepts
// in upper or lower case, such as "@Daily". Python passes such a preset to croniter unchanged
// unless the preset is a key of cronPresets, and a Go Dag does the same.
var croniterAliases = map[string]bool{
	"@midnight": true,
	"@hourly":   true,
	"@daily":    true,
	"@weekly":   true,
	"@monthly":  true,
	"@yearly":   true,
	"@annually": true,
}

// cronItem matches one comma-separated item of a field of a cron expression. An item is one value,
// or values joined by -, / or #. A value is one of these:
//   - a number, with or without an L or a W before or after it, such as 15W or L5
//   - one of *, ?, L, W and R
//   - R with a range, such as R(0-30)
//   - three letters, as in a month or weekday name such as JAN or MON
//
// cronItem checks only the shape of an item. It takes some items that croniter rejects, such as a
// minute of 61 or a weekday of 5L. croniter rejects those only when Airflow tries to schedule the
// Dag.
var cronItem = regexp.MustCompile(`^(?i:` + cronValue + `(?:[-/#]` + cronValue + `)*)$`)

const cronValue = `(?:[0-9]+[LW]?|[LW][0-9]+|[*?LWR]|R\([0-9]+-[0-9]+\)|[A-Z]{3})`

// checkSchedule returns an error unless the Schedule is empty, a preset, or shaped like a cron
// expression of five to seven fields. Airflow does not check a cron expression when it loads a
// serialized Dag. Without this check, a Schedule such as "every day" would reach Airflow, and
// Airflow would never schedule the Dag, with only a line in its log to say why.
func checkSchedule(schedule string) error {
	switch schedule {
	case "", "@once", "@continuous":
		return nil
	}
	if _, ok := cronPresets[schedule]; ok || croniterAliases[strings.ToLower(schedule)] {
		return nil
	}
	fields := strings.Fields(schedule)
	shaped := len(fields) >= 5 && len(fields) <= 7
	for _, field := range fields {
		for item := range strings.SplitSeq(field, ",") {
			shaped = shaped && cronItem.MatchString(item)
		}
	}
	if !shaped {
		return fmt.Errorf(
			"airflow.DagSpec.Schedule is %q, which is not a cron expression or a preset; set a "+
				"cron expression such as \"0 3 * * *\", a preset such as \"@daily\", \"@once\" or "+
				"\"@continuous\", or no Schedule for a Dag that runs only when something triggers it",
			schedule,
		)
	}
	return nil
}

// serializeTimetable returns the timetable that Python's DAG builds from the same schedule string.
// Dag has already checked the Schedule with checkSchedule.
func serializeTimetable(schedule string) map[string]any {
	switch schedule {
	case "":
		return map[string]any{"__type": nullTimetable, "__var": map[string]any{}}
	case "@once":
		return map[string]any{"__type": onceTimetable, "__var": map[string]any{}}
	case "@continuous":
		return map[string]any{"__type": continuousTimetable, "__var": map[string]any{}}
	}
	expression := schedule
	if preset, ok := cronPresets[schedule]; ok {
		expression = preset
	}
	return map[string]any{
		"__type": cronTriggerTimetable,
		"__var": map[string]any{
			"expression":      expression,
			"timezone":        "UTC",
			"interval":        0.0,
			"run_immediately": false,
		},
	}
}

// schemaField is the property of the serialization schema that one field of DagSpec, TaskSpec or
// TaskGroupSpec sets. genspec writes a table of them for each struct in spec_fields.gen.go,
// keyed by the name of the field.
type schemaField struct {
	key string
	// schemaDefault is the default that the schema gives the property, or nil when the schema has
	// none. writeSpecFields leaves out a value equal to the default, because Airflow reads a missing
	// property as its schema default.
	schemaDefault any
}

// specRules says how the serializer writes the fields of one spec struct. A field that has no entry
// in fields panics, so a field that the generator adds to a struct is never left out unnoticed.
type specRules struct {
	fields map[string]schemaField
	// set names the lists that Python keeps in a set, so that Python writes the list sorted and
	// without repeats. writeSpecFields writes it the same way.
	set map[string]bool
	// skip names the fields that writeSpecFields never writes. The comment on each says why.
	skip map[string]bool
}

var dagSpecRules = specRules{
	fields: dagSpecFields,
	set:    map[string]bool{"Tags": true},
	skip: map[string]bool{
		// serializeTaskLocked writes Queue as the queue of each Go task whose TaskSpec sets no Queue.
		"Queue": true,
		// serializeTimetable writes Schedule as the timetable.
		"Schedule": true,
	},
}

var taskSpecRules = specRules{
	fields: taskSpecFields,
	skip: map[string]bool{
		// Python writes email_on_failure and email_on_retry only for an operator that has an email
		// recipient, and a TaskSpec has no field for a recipient.
		"EmailOnFailure": true,
		"EmailOnRetry":   true,
		// serializeTaskLocked writes the task_id of the TaskRef, where the group_ids of the groups that
		// hold the task prefix it.
		"TaskID": true,
	},
}

// The schema gives no property of a task group a default. serializeTaskGroup starts each group
// from the defaults of Python's TaskGroup instead.
var taskGroupSpecRules = specRules{fields: taskGroupSpecFields}

// writeSpecFields writes into data each field of spec that rules does not skip. It leaves out a
// field that is unset or that holds its schema default. A field is unset when it holds its zero
// value. A pointer field is unset only when it is nil, so a pointer field can set the zero value of
// the type it points to, such as false or 0.
func writeSpecFields(data map[string]any, spec any, rules specRules) {
	value := reflect.ValueOf(spec)
	for i := range value.NumField() {
		name := value.Type().Field(i).Name
		field, ok := rules.fields[name]
		if !ok {
			panic(fmt.Sprintf(
				"airflow: the serializer has no schema field for %s.%s", value.Type().Name(), name,
			))
		}
		if rules.skip[name] {
			continue
		}
		encoded, set := encodeSpecValue(value.Field(i), rules.set[name])
		if !set || (field.schemaDefault != nil && isSameJSON(encoded, field.schemaDefault)) {
			continue
		}
		data[field.key] = encoded
	}
}

// encodeSpecValue returns the value of a spec field in the form that Python's serializer writes for
// the property, or false when the field is unset. set says that Python keeps the list in a set.
func encodeSpecValue(value reflect.Value, set bool) (any, bool) {
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, false
		}
		value = value.Elem()
	} else if value.IsZero() {
		return nil, false
	}
	switch v := value.Interface().(type) {
	case time.Time:
		// The zero Time moved to a location with In is not the zero value of time.Time, so value.IsZero
		// above misses it.
		if v.IsZero() {
			return nil, false
		}
		return encodeTime(v), true
	case time.Duration:
		return encodeDuration(v), true
	}
	switch value.Kind() {
	case reflect.String:
		return value.String(), true
	case reflect.Bool:
		return value.Bool(), true
	case reflect.Int:
		return int(value.Int()), true
	case reflect.Float64:
		return value.Float(), true
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.String {
			items := make([]string, value.Len())
			for i := range items {
				items[i] = value.Index(i).String()
			}
			if set {
				slices.Sort(items)
				items = slices.Compact(items)
			}
			return items, true
		}
	}
	panic(fmt.Sprintf("airflow: the serializer cannot write a spec field of type %s", value.Type()))
}

// isSameJSON reports whether two encoded values are the same JSON value. Like Python, it takes 1
// and 1.0 for the same number.
func isSameJSON(a, b any) bool {
	if x, ok := jsonNumber(a); ok {
		y, ok := jsonNumber(b)
		return ok && x == y
	}
	return a == b
}

func jsonNumber(value any) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case float64:
		return v, true
	}
	return 0, false
}

// encodeTime returns t as Python's serializer writes a datetime: the seconds since the Unix epoch,
// to the microsecond. A Python datetime holds nothing finer than a microsecond.
func encodeTime(t time.Time) float64 { return float64(t.UnixMicro()) / 1e6 }

// checkTime returns an error for a time that a Python datetime cannot hold, which is a time whose
// year in UTC is not from 1 to 9999. Airflow would reject a serialized Dag with such a time when it
// loads the Dag. field names the field that holds t. The zero Time passes, because it means that
// the field is unset.
func checkTime(field string, t time.Time) error {
	if year := t.UTC().Year(); !t.IsZero() && (year < 1 || year > 9999) {
		return fmt.Errorf(
			"%s is %s in UTC; Airflow takes a time only from year 1 to year 9999",
			field, t.UTC().Format(time.RFC3339Nano),
		)
	}
	return nil
}

// encodeDuration returns d as Python's serializer writes a timedelta: a number of seconds, to the
// microsecond. A Python timedelta holds nothing finer than a microsecond.
func encodeDuration(d time.Duration) float64 { return float64(d.Microseconds()) / 1e6 }

// serialize returns the serialized Dag that Airflow stores for d. A DagFileParsingResult carries it
// as the data of one entry of serialized_dags. Python's DagSerialization.to_dict returns the same
// shape for a Dag authored in Python. fileloc is the path of the file that declares the Dag, and
// relativeFileloc is that path relative to the root of its Dag bundle.
//
// serialize panics unless d is registered. Registration expands each edge to or from a task group
// into edges between tasks, which the downstream_task_ids of a serialized Dag hold.
func (d *DagRef) serialize(fileloc, relativeFileloc string) map[string]any {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.registered {
		panic(fmt.Sprintf(
			"airflow: Dag %q is not registered, so its group edges are not expanded yet", d.dagID,
		))
	}
	dag := map[string]any{
		"dag_id":           d.dagID,
		"fileloc":          fileloc,
		"relative_fileloc": relativeFileloc,
		// A Go Dag has no timezone of its own. Airflow reads its cron expression in UTC, the timezone
		// that serializeTimetable writes.
		"timezone":         "UTC",
		"timetable":        serializeTimetable(d.spec.Schedule),
		"tasks":            d.serializeTasksLocked(),
		"dag_dependencies": d.serializeDagDependenciesLocked(),
		"task_group":       d.serializeTaskGroupsLocked(),
		"edge_info":        d.serializeEdgeInfoLocked(),
		// Python's serializer writes params, deadline and allowed_run_types for every Dag. A Go Dag
		// cannot set any of them yet.
		"params":            []any{},
		"deadline":          nil,
		"allowed_run_types": nil,
	}
	// writeSpecFields leaves out each field that the Dag does not set. That includes max_active_tasks,
	// max_active_runs, max_consecutive_failed_dag_runs, catchup and disable_bundle_versioning, which a
	// Python Dag takes from the Airflow config when it does not set them. A bundle cannot read the
	// config, so Airflow fills those five in from its own config when it receives the Dag.
	writeSpecFields(dag, d.spec, dagSpecRules)
	return map[string]any{"__version": dagSerializationVersion, "dag": dag}
}

// serializeTasksLocked writes the tasks of d in the order they were added, as Python writes the
// tasks of a Dag. The caller holds d.mu.
func (d *DagRef) serializeTasksLocked() []any {
	tasks := make([]any, len(d.tasks))
	for i, task := range d.tasks {
		tasks[i] = d.serializeTaskLocked(task)
	}
	return tasks
}

// serializeTaskLocked writes one task of d. The caller holds d.mu.
func (d *DagRef) serializeTaskLocked(task *TaskRef) map[string]any {
	data := map[string]any{"task_id": task.taskID}
	spec := task.spec
	if task.triggerDagRun != nil {
		writeTriggerDagRun(data, *task.triggerDagRun)
	} else {
		data["task_type"] = goTaskType
		data["_task_module"] = goTaskModule
		data["language"] = goLanguage
		// Python writes template_fields for every operator. A Go task has no template fields, so the list
		// is empty.
		data["template_fields"] = []string{}
		// is_stub makes the API server send the task its arguments from _arg_bindings when the
		// task runs, as it does for a @task.stub task of a Python Dag.
		data["is_stub"] = true
		if len(task.inputs) > 0 {
			data["_arg_bindings"] = serializeArgBindings(task.inputs)
		}
		if task.decider != nil {
			// Python writes _can_skip_downstream for a branch operator. Airflow reads the skipmixin_key XCom
			// of the condition only when the flag is set, before it runs a task that comes after the
			// condition.
			data["_can_skip_downstream"] = true
		}
		if spec.Queue == "" {
			spec.Queue = d.spec.Queue
		}
	}
	writeSpecFields(data, spec, taskSpecRules)
	if len(task.downstreams) > 0 {
		downstreams := taskIDs(task.downstreams)
		slices.Sort(downstreams)
		data["downstream_task_ids"] = downstreams
	}
	return map[string]any{"__type": "operator", "__var": data}
}

// serializeArgBindings returns the arg bindings for the tasks that Inputs passed. When the task
// runs, the API server sends it these bindings, one for each parameter after the Context, in order.
// Package reflect cannot read the names of the parameters of a Go function, and a Go task takes its
// arguments by position. So each binding is named after the position of its parameter: arg0 for the
// first parameter after the Context, arg1 for the next, and so on.
func serializeArgBindings(inputs []*TaskRef) []any {
	bindings := make([]any, len(inputs))
	for i, upstream := range inputs {
		bindings[i] = map[string]any{
			"name":    "arg" + strconv.Itoa(i),
			"kind":    "xcom",
			"task_id": upstream.taskID,
		}
	}
	return bindings
}

// writeTriggerDagRun writes a task from TriggerDagRun as Python's serializer writes a
// TriggerDagRunOperator, because a Python worker runs the task.
//
// For a Python Dag, Python's serializer writes the template fields of the operator and leaves out
// its other parameters. A Python worker gets those other parameters by parsing the Python Dag file
// again. A Go Dag has no Python Dag file, so writeTriggerDagRun also writes each other parameter
// that spec sets, under the name that TriggerDagRunOperator gives the parameter.
func writeTriggerDagRun(data map[string]any, spec TriggerDagRunSpec) {
	data["task_type"] = triggerDagRunTaskType
	data["_task_module"] = triggerDagRunTaskModule
	data["ui_color"] = triggerDagRunUIColor
	data["template_fields"] = slices.Clone(triggerDagRunTemplateFields)
	data["template_fields_renderers"] = map[string]any{"conf": "py"}
	data["_operator_extra_links"] = map[string]any{"Triggered DAG": "_link_TriggerDagRunLink"}

	data["trigger_dag_id"] = spec.DagID
	if spec.RunID != "" {
		data["trigger_run_id"] = spec.RunID
	}
	// Python leaves out a template field that is None, but the default logical_date is NOTSET, a
	// sentinel that lets TriggerDagRunOperator pick the logical date. Python writes the sentinel as
	// the string NOTSET, and Airflow loads the string as it is.
	data["logical_date"] = cmp.Or(spec.LogicalDate, "NOTSET")
	if spec.Conf != nil {
		data["conf"] = copyJSON(spec.Conf)
	}
	data["wait_for_completion"] = spec.WaitForCompletion
	data["skip_when_already_exists"] = spec.SkipWhenAlreadyExists

	if !spec.RunAfter.IsZero() {
		// Airflow decodes a property that is not a template field with BaseSerialization.deserialize. It
		// returns a datetime only for a value written as {"__type": "datetime", ...}, not for a bare
		// number of seconds.
		data["run_after"] = map[string]any{"__type": "datetime", "__var": encodeTime(spec.RunAfter)}
	}
	if spec.ResetDagRun {
		data["reset_dag_run"] = true
	}
	if spec.PokeInterval != nil {
		data["poke_interval"] = int(*spec.PokeInterval / time.Second)
	}
	if len(spec.AllowedStates) > 0 {
		data["allowed_states"] = dagRunStateNames(spec.AllowedStates)
	}
	// An empty FailedStates that is not nil means that no Dag run state fails the task. So
	// FailedStates is written whenever it is not nil, even when it is empty.
	if spec.FailedStates != nil {
		data["failed_states"] = dagRunStateNames(spec.FailedStates)
	}
	if spec.FailWhenDagIsPaused {
		data["fail_when_dag_is_paused"] = true
	}
	if spec.Note != "" {
		data["note"] = spec.Note
	}
	if spec.Deferrable != nil {
		data["deferrable"] = *spec.Deferrable
	}
}

func dagRunStateNames(states []DagRunState) []string {
	names := make([]string, len(states))
	for i, state := range states {
		names[i] = string(state)
	}
	return names
}

// copyJSON returns a copy of a value that copyConf made, so that a serialized Dag shares no map or
// slice with the Dag it comes from.
func copyJSON(value any) any {
	switch v := value.(type) {
	case map[string]any:
		copied := make(map[string]any, len(v))
		for key, item := range v {
			copied[key] = copyJSON(item)
		}
		return copied
	case []any:
		copied := make([]any, len(v))
		for i, item := range v {
			copied[i] = copyJSON(item)
		}
		return copied
	}
	return value
}

// serializeDagDependenciesLocked returns a dependency for each task from TriggerDagRun, on the Dag
// that the task triggers, sorted as Python sorts them. Python's serializer writes the same
// dependency for a TriggerDagRunOperator, and the Airflow UI draws the dependencies between Dags
// from these. The caller holds d.mu.
func (d *DagRef) serializeDagDependenciesLocked() []any {
	type dependency struct{ target, label, taskID string }
	var found []dependency
	for _, task := range d.tasks {
		if task.triggerDagRun == nil {
			continue
		}
		label := cmp.Or(task.spec.TaskDisplayName, task.taskID)
		found = append(found, dependency{task.triggerDagRun.DagID, label, task.taskID})
	}
	slices.SortFunc(found, func(a, b dependency) int {
		return cmp.Or(
			cmp.Compare(a.target, b.target),
			cmp.Compare(a.label, b.label),
			cmp.Compare(a.taskID, b.taskID),
		)
	})
	dependencies := make([]any, len(found))
	for i, dep := range found {
		dependencies[i] = map[string]any{
			"source":          d.dagID,
			"target":          dep.target,
			"label":           dep.label,
			"dependency_type": "trigger",
			"dependency_id":   dep.taskID,
		}
	}
	return dependencies
}

// serializeEdgeInfoLocked returns the labels of the edges of d, keyed by the ID of the upstream end
// and then by the ID of the downstream end, as Python's DAG.edge_info holds them. The Airflow UI
// draws an edge to or from a task group as an edge to or from a join node of the group. So the
// label of such an edge goes under the ID of the join node, where Python puts it too. The caller
// holds d.mu.
func (d *DagRef) serializeEdgeInfoLocked() map[string]any {
	info := map[string]any{}
	add := func(upstream, downstream, label string) {
		if label == "" {
			return
		}
		labels, ok := info[upstream].(map[string]any)
		if !ok {
			labels = map[string]any{}
			info[upstream] = labels
		}
		labels[downstream] = map[string]any{"label": label}
	}
	for key, label := range d.edgeLabels {
		add(key.upstream, key.downstream, label)
	}
	for _, edge := range d.groupEdges {
		key := edgeKey{upstream: edge.upstream.id(), downstream: edge.downstream.id()}
		upstream, downstream := key.upstream, key.downstream
		if edge.upstream.group != nil {
			upstream += downstreamJoinSuffix
		}
		if edge.downstream.group != nil {
			downstream += upstreamJoinSuffix
		}
		add(upstream, downstream, d.groupEdgeLabels[key])
	}
	return info
}

// serializeTaskGroupsLocked returns the task groups of d as the tree that Python writes. The root
// of the tree is the group that Python gives every Dag, which holds each task and group that no
// other group holds. The caller holds d.mu.
func (d *DagRef) serializeTaskGroupsLocked() map[string]any {
	edges := d.collectGroupEdgesLocked()
	var children []Node
	for _, task := range d.tasks {
		if task.group == nil {
			children = append(children, task)
		}
	}
	for _, group := range d.groups {
		if group.parent == nil {
			children = append(children, group)
		}
	}
	return serializeTaskGroup(nil, children, edges)
}

// serializeTaskGroup returns one task group as Python's TaskGroupSerialization writes it. children
// are the tasks and groups in the group, and each group among them is written in full in the
// children of the group that holds it. group is nil for the root group.
func serializeTaskGroup(
	group *TaskGroupRef, children []Node, edges map[*TaskGroupRef]*groupEdgeIDs,
) map[string]any {
	data := map[string]any{
		"_group_id":          nil,
		"group_display_name": "",
		"prefix_group_id":    true,
		"tooltip":            "",
		"ui_color":           defaultGroupUIColor,
		"ui_fgcolor":         defaultGroupUIFgColor,
	}
	if group != nil {
		// Python records the group_id that the group was created with, without the prefixes from the
		// groups that hold it. It works the prefixes out again from the place of the group in the tree.
		data["_group_id"] = group.groupID[strings.LastIndex(group.groupID, ".")+1:]
		writeSpecFields(data, group.spec, taskGroupSpecRules)
	}
	nodes := make(map[string]any, len(children))
	for _, child := range children {
		switch child := child.(type) {
		case *TaskRef:
			nodes[child.taskID] = []any{"operator", child.taskID}
		case *TaskGroupRef:
			nodes[child.groupID] = []any{"taskgroup", serializeTaskGroup(child, child.children, edges)}
		}
	}
	data["children"] = nodes
	own, ok := edges[group]
	if !ok {
		own = newGroupEdgeIDs()
	}
	data["upstream_group_ids"] = sortedIDs(own.upstreamGroups)
	data["downstream_group_ids"] = sortedIDs(own.downstreamGroups)
	data["upstream_task_ids"] = sortedIDs(own.upstreamTasks)
	data["downstream_task_ids"] = sortedIDs(own.downstreamTasks)
	return data
}

// groupEdgeIDs holds the group edges that Python records on a task group, by the ID of the task or
// group at the other end of each edge. The Airflow UI draws such an edge once, to or from the
// group, instead of drawing each edge between tasks that it stands for.
type groupEdgeIDs struct {
	upstreamGroups, downstreamGroups, upstreamTasks, downstreamTasks map[string]bool
}

func newGroupEdgeIDs() *groupEdgeIDs {
	return &groupEdgeIDs{
		upstreamGroups:   map[string]bool{},
		downstreamGroups: map[string]bool{},
		upstreamTasks:    map[string]bool{},
		downstreamTasks:  map[string]bool{},
	}
}

// sortedIDs returns the IDs in set, sorted as Python sorts them. It returns an empty slice for an
// empty set, because a nil slice would be written as null where Python writes [].
func sortedIDs(set map[string]bool) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// collectGroupEdgesLocked works out the edges that Python records on each task group for the group
// edges of d. It records them as Python's TaskGroup.update_relative does for an edge declared with
// >>, whichever of Before and After declared the edge:
//   - an edge from a task to a group records the task as an upstream task of the group
//   - an edge from a group to a task records the task as a downstream task of the group
//   - an edge between two groups records each group on the other, and records the tasks that the
//     edge leaves the upstream group from as upstream tasks of the downstream group
//
// For an edge between two groups that Python declares with <<, as in b << a, Python records the
// first tasks of b as downstream tasks of a instead of the last tasks of a as upstream tasks of b.
// The Airflow UI draws the same graph either way. The caller holds d.mu.
func (d *DagRef) collectGroupEdgesLocked() map[*TaskGroupRef]*groupEdgeIDs {
	edges := make(map[*TaskGroupRef]*groupEdgeIDs)
	of := func(group *TaskGroupRef) *groupEdgeIDs {
		ids, ok := edges[group]
		if !ok {
			ids = newGroupEdgeIDs()
			edges[group] = ids
		}
		return ids
	}
	for _, edge := range d.groupEdges {
		upstream, downstream := edge.upstream, edge.downstream
		switch {
		case upstream.group == nil:
			of(downstream.group).upstreamTasks[upstream.task.taskID] = true
		case downstream.group == nil:
			of(upstream.group).downstreamTasks[downstream.task.taskID] = true
		default:
			of(upstream.group).downstreamGroups[downstream.group.groupID] = true
			of(downstream.group).upstreamGroups[upstream.group.groupID] = true
			for _, task := range edge.upstreamTasks {
				of(downstream.group).upstreamTasks[task.taskID] = true
			}
		}
	}
	return edges
}
