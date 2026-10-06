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
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cleanRows(Context) error    { return nil }
func validateRows(Context) error { return nil }

// groupTask adds a task that passes no data to group, which is what an order-only edge connects.
func groupTask(t *testing.T, group *TaskGroupRef, taskID string) *TaskRef {
	t.Helper()
	return group.Task(ping, TaskSpec{TaskID: taskID})
}

func assertChildren(t *testing.T, group *TaskGroupRef, want ...Node) {
	t.Helper()
	require.Len(t, group.children, len(want))
	for i := range want {
		assert.Same(t, want[i], group.children[i], "child %d", i)
	}
}

func assertGroupEdgeLabel(t *testing.T, dag *DagRef, upstream, downstream, want string) {
	t.Helper()
	label, declared := dag.groupEdgeLabels[edgeKey{upstream: upstream, downstream: downstream}]
	require.True(t, declared, "the group edge %s -> %s was not declared", upstream, downstream)
	assert.Equal(t, want, label)
}

func TestTaskGroupPrefixesTheTaskIDs(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")

	read := group.Task(readRows)
	counted := group.Task(countRows, Inputs(read), TaskSpec{TaskID: "count"})

	assert.Equal(t, "transform.readRows", read.taskID)
	assert.Equal(t, "transform.count", counted.taskID)
	assert.Same(t, group, read.group)
	assertTasks(t, inputRefs(counted), read)
	assertChildren(t, group, read, counted)
	assertTasks(t, dag.tasks, read, counted)
	assert.Same(t, read, dag.tasksByID["transform.readRows"])
}

func TestTaskGroupsNest(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	cleaned := transform.Task(cleanRows)
	checks := transform.TaskGroup("checks")
	nulls := groupTask(t, checks, "nulls")

	assert.Equal(t, "transform.checks", checks.groupID)
	assert.Equal(t, "transform.checks.nulls", nulls.taskID)
	assert.Same(t, transform, checks.parent)
	assert.Nil(t, transform.parent)
	// The groups form a tree, which is what a serialized Dag carries in task_group.children.
	assertChildren(t, transform, cleaned, checks)
	assertChildren(t, checks, nulls)
	assert.Equal(t, []*TaskGroupRef{transform, checks}, dag.groups)
	assert.Same(t, checks, dag.groupsByID["transform.checks"])
}

// TestPrefixGroupIDFalseKeepsTheIDsOfWhatTheGroupHolds pins Python's rule: a group decides
// whether its own ID prefixes what is added through it, and its parent decides whether the
// parent's ID prefixes the group's.
func TestPrefixGroupIDFalseKeepsTheIDsOfWhatTheGroupHolds(t *testing.T) {
	dag := Dag("etl")
	noPrefix := false
	transform := dag.TaskGroup("transform")
	checks := transform.TaskGroup("checks", TaskGroupSpec{PrefixGroupID: &noPrefix})
	nulls := groupTask(t, checks, "nulls")
	inner := checks.TaskGroup("inner")

	assert.Equal(t, "transform.checks", checks.groupID)
	assert.Equal(t, "nulls", nulls.taskID)
	assert.Equal(t, "inner", inner.groupID)
}

// TestPrefixesFollowTheParentOfEachGroup pins Python's rule across three levels: mid keeps the
// IDs of what it holds as written, so inner is not prefixed, while inner prefixes its own task.
func TestPrefixesFollowTheParentOfEachGroup(t *testing.T) {
	dag := Dag("etl")
	noPrefix := false
	outer := dag.TaskGroup("outer")
	mid := outer.TaskGroup("mid", TaskGroupSpec{PrefixGroupID: &noPrefix})
	inner := mid.TaskGroup("inner")

	assert.Equal(t, "outer.mid", mid.groupID)
	assert.Equal(t, "inner", inner.groupID)
	assert.Equal(t, "inner.rows", groupTask(t, inner, "rows").taskID)
	assert.Equal(t, "mid_rows", groupTask(t, mid, "mid_rows").taskID)
	assert.Equal(t, "outer.rows", groupTask(t, outer, "rows").taskID)
}

// TestTaskGroupNamesATaskByItsPrefixedTaskID pins that an error raised before the task_id is
// settled names the task by the task_id that the group gives it.
func TestTaskGroupNamesATaskByItsPrefixedTaskID(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")

	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.Task: task "transform.v" of Dag "etl": got more than one `+
			`airflow.TaskSpec; set all of the task's attributes in one TaskSpec`,
		func() { group.Task(ping, TaskSpec{TaskID: "v"}, TaskSpec{}) },
	)
	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.Task: task "transform.ping" of Dag "etl": got more than one `+
			`airflow.Inputs; pass all of the task's inputs to one airflow.Inputs`,
		func() { group.Task(ping, Inputs(), Inputs()) },
	)
}

// TestTaskChecksTheTaskIDWithItsPrefixes pins Python's validate_key, which checks a task_id once
// the group_ids of its task groups prefix it: each group_id can be valid while the task_id they
// make is too long.
func TestTaskChecksTheTaskIDWithItsPrefixes(t *testing.T) {
	long := strings.Repeat("g", 200)
	for _, tc := range []struct {
		name  string
		build func(dag *DagRef)
		want  string
	}{
		{
			name: "too long once prefixed",
			build: func(dag *DagRef) {
				dag.TaskGroup(long).TaskGroup(long).Task(ping, TaskSpec{TaskID: "t"})
			},
			want: fmt.Sprintf(
				`airflow.TaskGroupRef.Task: Dag "etl": task_id %q has 403 characters, `+
					`counting the group_ids that prefix it, and a task_id has at most 250; `+
					`shorten a group_id, or set a shorter task_id with airflow.TaskSpec{TaskID: ...}`,
				long+"."+long+".t",
			),
		},
		{
			name:  "too long without a group",
			build: func(dag *DagRef) { dag.Task(ping, TaskSpec{TaskID: strings.Repeat("t", 251)}) },
			want: fmt.Sprintf(
				`airflow.DagRef.Task: Dag "etl": task_id %q has 251 characters, and a task_id `+
					`has at most 250; set a shorter one with airflow.TaskSpec{TaskID: ...}`,
				strings.Repeat("t", 251),
			),
		},
		{
			name:  "a character Python rejects",
			build: func(dag *DagRef) { dag.Task(ping, TaskSpec{TaskID: "clean rows"}) },
			want: `airflow.DagRef.Task: Dag "etl": task_id "clean rows" holds ' ', and a task_id ` +
				`holds only letters, digits, underscores, dashes and dots; set another one with ` +
				`airflow.TaskSpec{TaskID: ...}`,
		},
		{
			name:  "bytes that are not UTF-8",
			build: func(dag *DagRef) { dag.Task(ping, TaskSpec{TaskID: "rows\xff"}) },
			want: `airflow.DagRef.Task: Dag "etl": task_id "rows\xff" is not valid UTF-8; ` +
				`set another one with airflow.TaskSpec{TaskID: ...}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dag := Dag("etl")

			assert.PanicsWithValue(t, tc.want, func() { tc.build(dag) })
			assert.Empty(t, dag.tasks)
		})
	}
}

func TestTaskTakesTheTaskIDsPythonTakes(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup(strings.Repeat("g", 200))

	// The last one makes a task_id of exactly 250 characters.
	for _, taskID := range []string{"extract.rows", "clean-rows_2", "清理", strings.Repeat("t", 49)} {
		assert.NotPanics(t, func() { group.Task(ping, TaskSpec{TaskID: taskID}) }, taskID)
	}
}

func TestPrefixGroupIDTrueKeepsThePrefix(t *testing.T) {
	dag := Dag("etl")
	prefix := true
	group := dag.TaskGroup("transform", TaskGroupSpec{PrefixGroupID: &prefix})

	assert.Equal(t, "transform.cleanRows", group.Task(cleanRows).taskID)
}

func TestTaskGroupAddsAConditionInsideTheGroup(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("gate")
	loaded := dag.Task(load)

	condition := group.If(isReady).Then(loaded)

	assert.Equal(t, "gate.isReady", condition.task.taskID)
	assert.Same(t, group, condition.task.group)
	assertChildren(t, group, condition.task)
	assertTasks(t, loaded.upstreams, condition.task)
}

func TestTaskGroupIfRejectsATriggerDagRun(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("gate")

	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.If: Dag "etl": fn comes from airflow.TriggerDagRun, `+
			`but a condition function is a Go function that returns (bool, error)`,
		func() { group.If(TriggerDagRun(TriggerDagRunSpec{DagID: "other"})) },
	)
}

func TestTaskGroupAddsASwitchInsideTheGroup(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("route")
	long := group.Task(handleLong)
	short := dag.Task(handleShort)

	pick := group.Switch(pickPath).Case(long).Case(short)

	assert.Equal(t, "route.pickPath", pick.task.taskID)
	assert.Same(t, group, pick.task.group)
	assertChildren(t, group, long, pick.task)
	assertTasks(t, long.upstreams, pick.task)
	assertTasks(t, short.upstreams, pick.task)
}

func TestTaskGroupSwitchRejectsATriggerDagRun(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("route")

	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.Switch: Dag "etl": fn comes from airflow.TriggerDagRun, `+
			`but a decider function is a Go function that returns (*airflow.TaskRef, error)`,
		func() { group.Switch(TriggerDagRun(TriggerDagRunSpec{DagID: "other"})) },
	)
}

func TestTaskGroupTakesAtMostOneTaskGroupSpec(t *testing.T) {
	dag := Dag("etl")

	assert.PanicsWithValue(t,
		`airflow.DagRef.TaskGroup: task group "transform" of Dag "etl" got 2 `+
			`airflow.TaskGroupSpec values; set all of the group's attributes in one TaskGroupSpec`,
		func() { dag.TaskGroup("transform", TaskGroupSpec{}, TaskGroupSpec{}) },
	)
	assert.Empty(t, dag.groups)

	// The message names a nested group by the group_id it would get.
	transform := dag.TaskGroup("transform")
	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.TaskGroup: task group "transform.checks" of Dag "etl" got 2 `+
			`airflow.TaskGroupSpec values; set all of the group's attributes in one TaskGroupSpec`,
		func() { transform.TaskGroup("checks", TaskGroupSpec{}, TaskGroupSpec{}) },
	)
	assert.Empty(t, transform.children)
}

func TestTaskGroupCopiesItsSpec(t *testing.T) {
	dag := Dag("etl")
	noPrefix := false
	group := dag.TaskGroup("transform", TaskGroupSpec{PrefixGroupID: &noPrefix, Tooltip: "rows"})

	noPrefix = true

	assert.False(t, *group.spec.PrefixGroupID)
	assert.Equal(t, "rows", group.spec.Tooltip)
	assert.Equal(t, "cleanRows", group.Task(cleanRows).taskID)
}

func TestTaskGroupChecksTheGroupID(t *testing.T) {
	for _, tc := range []struct {
		name, groupID, want string
	}{
		{
			name:    "empty",
			groupID: "",
			want:    "a task group needs a group_id, and got an empty one",
		},
		{
			name:    "a dot",
			groupID: "transform.checks",
			want: `group_id "transform.checks" holds '.'; a group_id holds only letters, ` +
				`digits, underscores and dashes; nest a group in another with TaskGroupRef.TaskGroup`,
		},
		{
			name:    "a space",
			groupID: "clean rows",
			want: `group_id "clean rows" holds ' '; a group_id holds only letters, ` +
				`digits, underscores and dashes`,
		},
		{
			name:    "too long",
			groupID: strings.Repeat("g", 201),
			want: fmt.Sprintf(
				"group_id %q has 201 characters; a group_id has at most 200",
				strings.Repeat("g", 201),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dag := Dag("etl")

			assert.PanicsWithValue(t,
				`airflow.DagRef.TaskGroup: Dag "etl": `+tc.want,
				func() { dag.TaskGroup(tc.groupID) },
			)
			assert.Empty(t, dag.groups)
		})
	}
}

// TestTaskGroupTakesTheGroupIDsPythonTakes pins that a letter or a digit outside ASCII is a word
// character, as it is for Python's \w, and that the length is counted in characters.
func TestTaskGroupTakesTheGroupIDsPythonTakes(t *testing.T) {
	dag := Dag("etl")

	for _, groupID := range []string{"clean-rows_2", "清理", "étape٣", strings.Repeat("清", 200)} {
		assert.NotPanics(t, func() { dag.TaskGroup(groupID) }, groupID)
	}
}

func TestTaskGroupsAndTasksShareOneNamespace(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func(dag *DagRef)
		want  string
	}{
		{
			name:  "a group with the ID of a group",
			build: func(dag *DagRef) { dag.TaskGroup("transform"); dag.TaskGroup("transform") },
			want: `airflow.DagRef.TaskGroup: Dag "etl" cannot add task group "transform", ` +
				`because task group "transform" already takes the ID "transform"; ` +
				`pass another group_id`,
		},
		{
			name:  "a group with the ID of a task",
			build: func(dag *DagRef) { dag.Task(load); dag.TaskGroup("load") },
			want: `airflow.DagRef.TaskGroup: Dag "etl" cannot add task group "load", ` +
				`because task "load" already takes the ID "load"; pass another group_id`,
		},
		{
			name:  "a task with the ID of a group",
			build: func(dag *DagRef) { dag.TaskGroup("load"); dag.Task(load) },
			want: `airflow.DagRef.Task: Dag "etl" cannot add task "load", ` +
				`because task group "load" already takes the ID; ` +
				`set another task_id with airflow.TaskSpec{TaskID: ...}`,
		},
		{
			name: "a nested group with the ID of a task its prefix makes",
			build: func(dag *DagRef) {
				group := dag.TaskGroup("transform")
				group.Task(cleanRows)
				group.TaskGroup("cleanRows")
			},
			want: `airflow.TaskGroupRef.TaskGroup: Dag "etl" cannot add task group ` +
				`"transform.cleanRows", because task "transform.cleanRows" already takes the ID ` +
				`"transform.cleanRows"; pass another group_id`,
		},
		{
			name: "a task with the ID of a join node",
			build: func(dag *DagRef) {
				dag.TaskGroup("transform").Task(ping, TaskSpec{TaskID: "upstream_join_id"})
			},
			want: `airflow.TaskGroupRef.Task: Dag "etl" cannot add task ` +
				`"transform.upstream_join_id", because a join node of task group "transform" ` +
				`already takes the ID; set another task_id with airflow.TaskSpec{TaskID: ...}`,
		},
		{
			name: "a group whose join node has the ID of a task",
			build: func(dag *DagRef) {
				dag.Task(ping, TaskSpec{TaskID: "transform.downstream_join_id"})
				dag.TaskGroup("transform")
			},
			want: `airflow.DagRef.TaskGroup: Dag "etl" cannot add task group "transform", ` +
				`because task "transform.downstream_join_id" already takes the ID ` +
				`"transform.downstream_join_id"; pass another group_id`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t, tc.want, func() { tc.build(Dag("etl")) })
		})
	}
}

// TestTaskGroupsWithoutAPrefixShareTheirNamespace pins that a group which keeps the IDs of its
// tasks as written puts them in the namespace of the whole Dag.
func TestTaskGroupsWithoutAPrefixShareTheirNamespace(t *testing.T) {
	dag := Dag("etl")
	noPrefix := false
	dag.Task(load)
	group := dag.TaskGroup("transform", TaskGroupSpec{PrefixGroupID: &noPrefix})

	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.Task: Dag "etl" already has a task "load"; `+
			`set another task_id with airflow.TaskSpec{TaskID: ...}`,
		func() { group.Task(load) },
	)
	assert.Empty(t, group.children)
}

func TestTaskGroupMethodsRejectAGroupTheDagDidNotReturn(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")
	copied := *group

	for _, tc := range []struct {
		name string
		call func()
		want string
	}{
		{
			name: "Task on a copy",
			call: func() { copied.Task(cleanRows) },
			want: `airflow.TaskGroupRef.Task: Dag "etl" got a *airflow.TaskGroupRef ` +
				`that DagRef.TaskGroup or TaskGroupRef.TaskGroup did not return`,
		},
		{
			name: "TaskGroup on a copy",
			call: func() { copied.TaskGroup("checks") },
			want: `airflow.TaskGroupRef.TaskGroup: Dag "etl" got a *airflow.TaskGroupRef ` +
				`that DagRef.TaskGroup or TaskGroupRef.TaskGroup did not return`,
		},
		{
			name: "If on a copy",
			call: func() { copied.If(isReady) },
			want: `airflow.TaskGroupRef.If: Dag "etl" got a *airflow.TaskGroupRef ` +
				`that DagRef.TaskGroup or TaskGroupRef.TaskGroup did not return`,
		},
		{
			name: "Switch on a copy",
			call: func() { copied.Switch(pickPath) },
			want: `airflow.TaskGroupRef.Switch: Dag "etl" got a *airflow.TaskGroupRef ` +
				`that DagRef.TaskGroup or TaskGroupRef.TaskGroup did not return`,
		},
		{
			name: "Switch on a nil group",
			call: func() { (*TaskGroupRef)(nil).Switch(pickPath) },
			want: "airflow.TaskGroupRef.Switch: DagRef.TaskGroup or TaskGroupRef.TaskGroup " +
				"did not return the *airflow.TaskGroupRef",
		},
		{
			name: "Task on a zero group",
			call: func() { (&TaskGroupRef{}).Task(cleanRows) },
			want: "airflow.TaskGroupRef.Task: DagRef.TaskGroup or TaskGroupRef.TaskGroup " +
				"did not return the *airflow.TaskGroupRef",
		},
		{
			name: "TaskGroup on a nil group",
			call: func() { (*TaskGroupRef)(nil).TaskGroup("checks") },
			want: "airflow.TaskGroupRef.TaskGroup: DagRef.TaskGroup or TaskGroupRef.TaskGroup " +
				"did not return the *airflow.TaskGroupRef",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t, tc.want, tc.call)
		})
	}
	assert.Empty(t, group.children)
	assert.Empty(t, dag.tasks)
	assert.Equal(t, []*TaskGroupRef{group}, dag.groups)
}

func TestTaskGroupMethodsAfterRegisterPanic(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")
	group.Task(cleanRows)
	Bundle().Register(dag)
	registered := snapshot(dag)

	assert.PanicsWithValue(t,
		`airflow.DagRef.TaskGroup: Dag "etl" has already been registered; `+
			`add every task group before Register`,
		func() { dag.TaskGroup("load") },
	)
	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.TaskGroup: Dag "etl" has already been registered; `+
			`add every task group before Register`,
		func() { group.TaskGroup("checks") },
	)
	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.Task: Dag "etl" has already been registered; `+
			`add every task before Register`,
		func() { group.Task(validateRows) },
	)
	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.If: Dag "etl" has already been registered; `+
			`add every task before Register`,
		func() { group.If(isReady) },
	)
	assert.PanicsWithValue(t,
		`airflow.TaskGroupRef.Switch: Dag "etl" has already been registered; `+
			`add every task before Register`,
		func() { group.Switch(pickPath) },
	)
	assert.Equal(t, registered, snapshot(dag))
}

func TestTaskGroupIsSafeForConcurrentUse(t *testing.T) {
	const workers = 8
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")

	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			task := groupTask(t, group, fmt.Sprintf("task_%d", i))
			nested := group.TaskGroup(fmt.Sprintf("group_%d", i))
			groupTask(t, nested, "rows")
			extracted.Before(Label(nested, fmt.Sprintf("to %d", i)))
			nested.After(task)
		})
	}
	wg.Wait()
	Bundle().Register(dag)

	assert.Len(t, group.children, 2*workers)
	assert.Len(t, dag.tasks, 1+2*workers)
	assert.Len(t, dag.groups, 1+workers)
	assert.Len(t, dag.groupEdges, 2*workers)
	assert.Len(t, extracted.downstreams, workers)
}

func TestTaskGroupRefIsANode(t *testing.T) {
	var _ Node = (*TaskGroupRef)(nil)
}

// TestGroupBeforeATaskReachesTheLastTasksOfTheGroup pins Python's transform >> load: the edge
// runs from each task of the group that no other task of the group runs after.
func TestGroupBeforeATaskReachesTheLastTasksOfTheGroup(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	validated := groupTask(t, group, "validate")
	audited := groupTask(t, group, "audit")
	cleaned.Before(validated)
	loaded := orderedTask(t, dag, "load")

	group.Before(loaded)

	// The edge stays an edge to the group until Register.
	assert.Empty(t, loaded.upstreams)
	assertGroupEdgeLabel(t, dag, "transform", "load", "")

	Bundle().Register(dag)

	assertTasks(t, loaded.upstreams, validated, audited)
	assertTasks(t, cleaned.downstreams, validated)
	assertEdgeLabel(t, dag, "transform.validate", "load", "")
	assertEdgeLabel(t, dag, "transform.audit", "load", "")
}

func TestTaskBeforeAGroupReachesTheFirstTasksOfTheGroup(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	validated := groupTask(t, group, "validate")
	audited := groupTask(t, group, "audit")
	cleaned.Before(validated)

	group.After(extracted)
	Bundle().Register(dag)

	assertTasks(t, extracted.downstreams, cleaned, audited)
	assertTasks(t, validated.upstreams, cleaned)
}

func TestGroupBeforeAGroupConnectsTheLastTasksToTheFirstTasks(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	cleaned := groupTask(t, transform, "clean")
	validated := groupTask(t, transform, "validate")
	cleaned.Before(validated)
	publish := dag.TaskGroup("publish")
	loaded := groupTask(t, publish, "load")
	reported := groupTask(t, publish, "report")

	transform.Before(publish)
	Bundle().Register(dag)

	assertTasks(t, validated.downstreams, loaded, reported)
	assertTasks(t, loaded.upstreams, validated)
	assertTasks(t, reported.upstreams, validated)
	assertGroupEdgeLabel(t, dag, "transform", "publish", "")
}

// TestAGroupEdgeReachesTasksAddedAfterIt pins why Register, and not the edge verb, works out the
// tasks of a group edge. In Python, transform >> load reaches only the tasks the group had when >>
// ran.
func TestAGroupEdgeReachesTasksAddedAfterIt(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")
	loaded := orderedTask(t, dag, "load")
	group.Before(loaded)

	cleaned := groupTask(t, group, "clean")
	validated := groupTask(t, group, "validate")
	cleaned.Before(validated)
	Bundle().Register(dag)

	assertTasks(t, loaded.upstreams, validated)
}

func TestGroupEdgeVerbsReturnTheirArgumentSet(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	loaded := orderedTask(t, dag, "load")

	extracted.Before(group).Before(loaded)
	Bundle().Register(dag)

	assertTasks(t, cleaned.upstreams, extracted)
	assertTasks(t, cleaned.downstreams, loaded)
}

// TestGroupEdgesExpandInTheOrderTheyWereDeclared pins the rule that the TypeScript SDK applies as
// well: an edge between two groups inside transform, declared after the edges to and from
// transform, does not change which tasks of transform those edges reach. Python's
// extract >> transform >> load followed by clean >> check gives the same five edges.
func TestGroupEdgesExpandInTheOrderTheyWereDeclared(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	transform := dag.TaskGroup("transform")
	clean := transform.TaskGroup("clean")
	cleaned := groupTask(t, clean, "rows")
	check := transform.TaskGroup("check")
	checked := groupTask(t, check, "rows")

	extracted.Before(transform).Before(loaded)
	clean.Before(check)
	Bundle().Register(dag)

	assertTasks(t, extracted.downstreams, cleaned, checked)
	assertTasks(t, cleaned.downstreams, loaded, checked)
	assertTasks(t, checked.downstreams, loaded)
}

// TestAGroupEdgeInsideAGroupDeclaredFirstShapesTheGroupsEnds is the same Dag with the edge between
// the two inner groups declared first. It makes clean.rows the only first task of transform and
// check.rows the only last one.
func TestAGroupEdgeInsideAGroupDeclaredFirstShapesTheGroupsEnds(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	transform := dag.TaskGroup("transform")
	clean := transform.TaskGroup("clean")
	cleaned := groupTask(t, clean, "rows")
	check := transform.TaskGroup("check")
	checked := groupTask(t, check, "rows")

	clean.Before(check)
	extracted.Before(transform).Before(loaded)
	Bundle().Register(dag)

	assertTasks(t, extracted.downstreams, cleaned)
	assertTasks(t, cleaned.downstreams, checked)
	assertTasks(t, loaded.upstreams, checked)
}

// TestAGroupEdgeLeavesTheLabelOfATaskEdgeAlone pins that a group edge keeps the label of a task edge
// that was declared on its own. Python puts the group edge's label over "rows" when the group edge
// is declared last.
func TestAGroupEdgeLeavesTheLabelOfATaskEdgeAlone(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	validated := groupTask(t, group, "validate")
	extracted.Before(Label(validated, "rows"))

	extracted.Before(Label(group, "to transform"))
	Bundle().Register(dag)

	assertGroupEdgeLabel(t, dag, "extract", "transform", "to transform")
	assertEdgeLabel(t, dag, "extract", "transform.clean", "to transform")
	assertEdgeLabel(t, dag, "extract", "transform.validate", "rows")
	assertTasks(t, extracted.downstreams, validated, cleaned)
}

// TestALabelOnAGroupEdge pins that a label on an edge to or from a group labels that edge,
// whichever end the label wraps, and labels the edges between tasks that it stands for only on an
// edge from a task to a group, as Python's extract >> Label("x") >> transform does.
func TestALabelOnAGroupEdge(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		declare              func(extracted *TaskRef, transform, publish *TaskGroupRef)
		upstream, downstream string
		taskEdgeLabel        string
	}{
		{
			name: "a task before a labelled group",
			declare: func(extracted *TaskRef, transform, _ *TaskGroupRef) {
				extracted.Before(Label(transform, "x"))
			},
			upstream: "extract", downstream: "transform",
			taskEdgeLabel: "x",
		},
		{
			name: "a task after a labelled group",
			declare: func(extracted *TaskRef, transform, _ *TaskGroupRef) {
				extracted.After(Label(transform, "x"))
			},
			upstream: "transform", downstream: "extract",
		},
		{
			name: "a group before a labelled task",
			declare: func(extracted *TaskRef, transform, _ *TaskGroupRef) {
				transform.Before(Label(extracted, "x"))
			},
			upstream: "transform", downstream: "extract",
		},
		{
			name: "a group after a labelled task",
			declare: func(extracted *TaskRef, transform, _ *TaskGroupRef) {
				transform.After(Label(extracted, "x"))
			},
			upstream: "extract", downstream: "transform",
			taskEdgeLabel: "x",
		},
		{
			name: "a group before a labelled group",
			declare: func(_ *TaskRef, transform, publish *TaskGroupRef) {
				transform.Before(Label(publish, "x"))
			},
			upstream: "transform", downstream: "publish",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dag := Dag("etl")
			extracted := orderedTask(t, dag, "extract")
			transform := dag.TaskGroup("transform")
			groupTask(t, transform, "clean")
			groupTask(t, transform, "validate")
			publish := dag.TaskGroup("publish")
			groupTask(t, publish, "push")

			tc.declare(extracted, transform, publish)
			Bundle().Register(dag)

			assertGroupEdgeLabel(t, dag, tc.upstream, tc.downstream, "x")
			require.Len(t, dag.edgeLabels, 2)
			for key, label := range dag.edgeLabels {
				assert.Equal(t, tc.taskEdgeLabel, label, "%s -> %s", key.upstream, key.downstream)
			}
		})
	}
}

// TestALabelOnAnEdgeBetweenTasksOfDifferentGroupsKeepsTheTaskEdge pins that a labelled edge
// between tasks in different task groups stays an edge between the two tasks. Python replaces the
// receiver with its group, which here would run load after transform.validate instead of after
// transform.clean.
func TestALabelOnAnEdgeBetweenTasksOfDifferentGroupsKeepsTheTaskEdge(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	cleaned := groupTask(t, transform, "clean")
	validated := groupTask(t, transform, "validate")
	cleaned.Before(validated)
	loaded := orderedTask(t, dag, "load")

	cleaned.Before(Label(loaded, "rows"))
	Bundle().Register(dag)

	assertEdgeLabel(t, dag, "transform.clean", "load", "rows")
	assertTasks(t, loaded.upstreams, cleaned)
	assert.Empty(t, dag.groupEdges)
}

// TestALabelOnAGroupEdgeFromAGroupedTaskStaysOnTheGroupEdge pins that a group edge from a task in a
// group labels none of the task edges it stands for. Python would replace the task with its group.
func TestALabelOnAGroupEdgeFromAGroupedTaskStaysOnTheGroupEdge(t *testing.T) {
	dag := Dag("etl")
	extract := dag.TaskGroup("extract")
	pulled := groupTask(t, extract, "pull")
	transform := dag.TaskGroup("transform")
	groupTask(t, transform, "clean")

	pulled.Before(Label(transform, "rows"))
	Bundle().Register(dag)

	assertGroupEdgeLabel(t, dag, "extract.pull", "transform", "rows")
	assertEdgeLabel(t, dag, "extract.pull", "transform.clean", "")
}

func TestRedeclaringAGroupEdgeIsIdempotent(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	groupTask(t, group, "clean")

	extracted.Before(Label(group, "rows"))
	group.After(extracted)

	require.Len(t, dag.groupEdges, 1)
	assertGroupEdgeLabel(t, dag, "extract", "transform", "rows")
}

func TestGroupEdgeVerbsRejectAnEdgeInsideTheGroup(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	cleaned := groupTask(t, transform, "clean")
	checks := transform.TaskGroup("checks")
	nulls := groupTask(t, checks, "nulls")

	for _, tc := range []struct {
		name string
		call func()
		want string
	}{
		{
			name: "a group before itself",
			call: func() { transform.Before(transform) },
			want: `airflow.Node.Before: Dag "etl": task group "transform" cannot depend on itself`,
		},
		{
			name: "a group before a task it holds",
			call: func() { transform.Before(cleaned) },
			want: `airflow.Node.Before: Dag "etl": task "transform.clean" is inside task group ` +
				`"transform", so an edge cannot connect them; ` +
				`an edge connects a group to a task or a group outside it`,
		},
		{
			name: "a task before the group that holds it",
			call: func() { cleaned.Before(transform) },
			want: `airflow.Node.Before: Dag "etl": task "transform.clean" is inside task group ` +
				`"transform", so an edge cannot connect them; ` +
				`an edge connects a group to a task or a group outside it`,
		},
		{
			name: "a group after a group it holds",
			call: func() { transform.After(checks) },
			want: `airflow.Node.After: Dag "etl": task group "transform.checks" is inside task ` +
				`group "transform", so an edge cannot connect them; ` +
				`an edge connects a group to a task or a group outside it`,
		},
		{
			name: "a task deeper inside the group",
			call: func() { nulls.After(transform) },
			want: `airflow.Node.After: Dag "etl": task "transform.checks.nulls" is inside task ` +
				`group "transform", so an edge cannot connect them; ` +
				`an edge connects a group to a task or a group outside it`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t, tc.want, tc.call)
		})
	}
	assert.Empty(t, dag.groupEdges)
	assert.Empty(t, dag.edgeLabels)
}

// TestGroupEdgeVerbsTakeAnEdgeBetweenGroupsOfOneParent pins that only holding the other end
// rules an edge out: two groups inside one group, and a group and a task beside it, can be
// ordered.
func TestGroupEdgeVerbsTakeAnEdgeBetweenGroupsOfOneParent(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	cleaned := groupTask(t, transform, "clean")
	checks := transform.TaskGroup("checks")
	groupTask(t, checks, "nulls")
	audits := transform.TaskGroup("audits")
	groupTask(t, audits, "rows")

	assert.NotPanics(t, func() { cleaned.Before(checks) })
	assert.NotPanics(t, func() { checks.Before(audits) })
	assertGroupEdgeLabel(t, dag, "transform.clean", "transform.checks", "")
	assertGroupEdgeLabel(t, dag, "transform.checks", "transform.audits", "")
}

func TestGroupEdgeVerbsRejectAGroupTheDagDidNotReturn(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	copied := *group
	other := Dag("other").TaskGroup("transform")

	for _, tc := range []struct {
		name string
		call func()
		want string
	}{
		{
			name: "a copy",
			call: func() { extracted.Before(&copied) },
			want: `airflow.Node.Before: Dag "etl" got a *airflow.TaskGroupRef ` +
				`that DagRef.TaskGroup or TaskGroupRef.TaskGroup did not return`,
		},
		{
			name: "a zero group",
			call: func() { extracted.Before(&TaskGroupRef{}) },
			want: "airflow.Node.Before: got a *airflow.TaskGroupRef that DagRef.TaskGroup or " +
				"TaskGroupRef.TaskGroup did not return",
		},
		{
			name: "a nil group",
			call: func() { extracted.After((*TaskGroupRef)(nil)) },
			want: "airflow.Node.After: nodes[0] is a nil *airflow.TaskGroupRef",
		},
		{
			name: "a nil group as the receiver",
			call: func() { (*TaskGroupRef)(nil).Before(extracted) },
			want: "airflow.Node.Before: the receiver is a nil *airflow.TaskGroupRef",
		},
		{
			name: "a group of another Dag",
			call: func() { extracted.Before(other) },
			want: `airflow.Node.Before: cannot declare an edge between task "extract" of Dag ` +
				`"etl" and task group "transform" of Dag "other"; ` +
				`an edge connects the tasks and task groups of one Dag`,
		},
		{
			name: "a nil group in a label",
			call: func() { Label((*TaskGroupRef)(nil), "rows") },
			want: "airflow.Label: node is a nil *airflow.TaskGroupRef",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t, tc.want, tc.call)
		})
	}
	assert.Empty(t, dag.groupEdges)
}

func TestGroupEdgeVerbsAfterRegisterPanic(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	groupTask(t, group, "clean")
	Bundle().Register(dag)

	assert.PanicsWithValue(t,
		`airflow.Node.After: Dag "etl" has already been registered; `+
			`declare every edge before Register`,
		func() { group.After(extracted) },
	)
	assert.Empty(t, dag.groupEdges)
}

// TestAnEdgeStepsOverAGroupWithNoTask pins Python's extract >> staging >> load for a staging that
// holds no task, which the TypeScript SDK serializes the same way: load runs after extract.
func TestAnEdgeStepsOverAGroupWithNoTask(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	staging := dag.TaskGroup("staging")
	// A group that holds only a group with no task holds no task either.
	staging.TaskGroup("rows")

	extracted.Before(staging).Before(loaded)
	Bundle().Register(dag)

	assertTasks(t, extracted.downstreams, loaded)
	assertTasks(t, loaded.upstreams, extracted)
}

// TestAnEdgeOutOfAnEmptyNestedGroupLeavesFromTheGroupHoldingIt pins Python's find_leaves, which
// stands an empty group in for its parent. An edge into an empty group has no such step.
func TestAnEdgeOutOfAnEmptyNestedGroupLeavesFromTheGroupHoldingIt(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	outer := dag.TaskGroup("outer")
	held := groupTask(t, outer, "t")
	spare := outer.TaskGroup("spare")

	spare.Before(loaded)
	extracted.Before(outer)
	Bundle().Register(dag)

	assertTasks(t, held.downstreams, loaded)
	assertTasks(t, loaded.upstreams, held)
}

func TestAnEdgeIntoAnEmptyNestedGroupDoesNotStepToTheGroupHoldingIt(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	outer := dag.TaskGroup("outer")
	groupTask(t, outer, "t")
	spare := outer.TaskGroup("spare")

	extracted.Before(spare)
	Bundle().Register(dag)

	assert.Empty(t, extracted.downstreams)
}

func TestAnEdgeStepsOverGroupsWithNoTaskInARow(t *testing.T) {
	dag := Dag("etl")
	transform := dag.TaskGroup("transform")
	cleaned := groupTask(t, transform, "clean")
	validated := groupTask(t, transform, "validate")
	first := dag.TaskGroup("first")
	second := dag.TaskGroup("second")
	publish := dag.TaskGroup("publish")
	pushed := groupTask(t, publish, "push")

	transform.Before(first).Before(second).Before(publish)
	Bundle().Register(dag)

	assertTasks(t, pushed.upstreams, cleaned, validated)
	assertTasks(t, cleaned.downstreams, pushed)
}

// TestAnEdgeStepsOverALoopOfGroupsWithNoTask pins that stepping over groups with no task ends
// when their group edges loop back. Without that, Register would recurse until the stack runs out.
func TestAnEdgeStepsOverALoopOfGroupsWithNoTask(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	first := dag.TaskGroup("first")
	second := dag.TaskGroup("second")

	extracted.Before(first).Before(second).Before(first)
	second.Before(loaded)
	Bundle().Register(dag)

	assertTasks(t, extracted.downstreams, loaded)
	assertTasks(t, loaded.upstreams, extracted)
}

func TestAnEdgeToAGroupWithNoTaskAndNothingBeyondItAddsNoEdge(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	staging := dag.TaskGroup("staging")

	extracted.Before(staging)
	Bundle().Register(dag)

	assert.Empty(t, extracted.downstreams)
	assertGroupEdgeLabel(t, dag, "extract", "staging", "")
}

func TestRegisterRejectsACycleThroughAGroupWithNoTask(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	staging := dag.TaskGroup("staging")
	extracted.Before(staging).Before(extracted)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`extract -> extract, through the group edge extract -> staging`,
		func() { Bundle().Register(dag) },
	)
	assert.Empty(t, extracted.downstreams)
	assert.Empty(t, extracted.upstreams)
	assert.Empty(t, dag.edgeLabels)
}

func TestRegisterTakesAGroupWithNoTaskThatNoEdgeReaches(t *testing.T) {
	dag := Dag("etl")
	orderedTask(t, dag, "extract")
	dag.TaskGroup("staging")

	assert.NotPanics(t, func() { Bundle().Register(dag) })
}

func TestRegisterRejectsACycleThroughAGroup(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	loaded := orderedTask(t, dag, "load")
	extracted.Before(group).Before(loaded)
	loaded.Before(extracted)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`extract -> transform.clean -> load -> extract, `+
			`through the group edges extract -> transform, transform -> load`,
		func() { Bundle().Register(dag) },
	)
	// Register takes back out the edges that the group edges stand for, so the Dag holds only the
	// edges its author declared.
	assert.False(t, dag.registered)
	assertTasks(t, extracted.downstreams)
	assertTasks(t, cleaned.upstreams)
	assertTasks(t, cleaned.downstreams)
	assertTasks(t, loaded.upstreams)
	assertTasks(t, loaded.downstreams, extracted)
	assert.NotContains(
		t,
		dag.edgeLabels,
		edgeKey{upstream: "extract", downstream: "transform.clean"},
	)
}

// TestRegisterReportsADeclaredCycleAsDeclared pins that a cycle between edges the author declared
// is reported before any group edge adds edges to the graph, so the message shows the cycle as
// written rather than one that a group edge closes.
func TestRegisterReportsADeclaredCycleAsDeclared(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	validated := groupTask(t, group, "validate")
	cleaned.Before(validated).Before(cleaned)
	extracted.Before(group).Before(extracted)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`transform.clean -> transform.validate -> transform.clean`,
		func() { Bundle().Register(dag) },
	)
}

// TestRegisterReportsACycleInsideAGroupOnAnEdge pins that a group whose tasks all sit on a cycle
// has no first or last task, so Register steps over it as it does a group with no task, and then
// reports the cycle.
func TestRegisterReportsACycleInsideAGroupOnAnEdge(t *testing.T) {
	dag := Dag("etl")
	group := dag.TaskGroup("transform")
	cleaned := groupTask(t, group, "clean")
	validated := groupTask(t, group, "validate")
	cleaned.Before(validated).Before(cleaned)
	loaded := orderedTask(t, dag, "load")
	group.Before(loaded)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: the task dependencies of Dag "etl" contain a cycle: `+
			`transform.clean -> transform.validate -> transform.clean`,
		func() { Bundle().Register(dag) },
	)
}

// TestRegisterTakesADagThatAnotherBundleRegistered pins that registering a Dag again, in another
// bundle, changes nothing about it.
func TestRegisterTakesADagThatAnotherBundleRegistered(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	group := dag.TaskGroup("transform")
	groupTask(t, group, "clean")
	extracted.Before(group)
	Bundle().Register(dag)
	registered := snapshot(dag)

	assert.NotPanics(t, func() { Bundle().Register(dag) })
	assert.Equal(t, registered, snapshot(dag))
}

// TestRegisterTakesAConditionOnceItHasATaskFromThen pins that a Dag that Register rejects for a
// condition without a task from Then can be completed and registered, with its group edges
// expanded then.
func TestRegisterTakesAConditionOnceItHasATaskFromThen(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	group := dag.TaskGroup("gate")
	condition := group.If(isReady)
	extracted.Before(group)

	assert.PanicsWithValue(t,
		`airflow.BundleRef.Register: condition "gate.isReady" of Dag "etl" has no task from `+
			`Then; name the task that runs when the condition is true with IfRef.Then`,
		func() { Bundle().Register(dag) },
	)
	assert.Empty(t, extracted.downstreams)

	condition.Then(loaded)
	Bundle().Register(dag)

	assertTasks(t, extracted.downstreams, condition.task)
	assertTasks(t, condition.task.downstreams, loaded)
}

func TestTasksOfAGroupTakeInputsAndConditionsFromOutsideIt(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	group := dag.TaskGroup("transform")
	counted := group.Task(countRows, Inputs(read))
	reported := dag.Task(report, Inputs(counted))
	gate := dag.If(isReady)
	cleaned := group.Task(cleanRows)
	gate.Then(cleaned)

	Bundle().Register(dag)

	assertTasks(t, counted.upstreams, read)
	assertTasks(t, reported.upstreams, counted)
	assertTasks(t, cleaned.upstreams, gate.task)
}

// dagSnapshot is what a Dag records, by ID, so that two snapshots are equal when the Dag records
// the same tasks, groups, inputs, conditions, switches and edges in the same order.
type dagSnapshot struct {
	registered             bool
	tasks, groups          []string
	taskKeys, groupKeys    []string
	children               map[string][]string
	inputs                 map[string][]string
	sides                  map[string][2]string
	cases                  map[string][]string
	upstreams, downstreams map[string][]string
	edgeLabels             map[edgeKey]string
	groupEdges             []edgeKey
	groupEdgeLabels        map[edgeKey]string
}

func snapshot(dag *DagRef) dagSnapshot {
	dag.mu.Lock()
	defer dag.mu.Unlock()

	s := dagSnapshot{
		registered:      dag.registered,
		taskKeys:        slices.Sorted(maps.Keys(dag.tasksByID)),
		groupKeys:       slices.Sorted(maps.Keys(dag.groupsByID)),
		children:        make(map[string][]string),
		inputs:          make(map[string][]string),
		sides:           make(map[string][2]string),
		cases:           make(map[string][]string),
		upstreams:       make(map[string][]string),
		downstreams:     make(map[string][]string),
		edgeLabels:      maps.Clone(dag.edgeLabels),
		groupEdgeLabels: maps.Clone(dag.groupEdgeLabels),
	}
	for _, task := range dag.tasks {
		s.tasks = append(s.tasks, task.taskID)
		s.inputs[task.taskID] = taskIDs(inputRefs(task))
		s.upstreams[task.taskID] = taskIDs(task.upstreams)
		s.downstreams[task.taskID] = taskIDs(task.downstreams)
		switch decider := task.decider.(type) {
		case *IfRef:
			var sides [2]string
			if decider.thenTask != nil {
				sides[0] = decider.thenTask.taskID
			}
			if decider.elseTask != nil {
				sides[1] = decider.elseTask.taskID
			}
			s.sides[task.taskID] = sides
		case *SwitchRef:
			s.cases[task.taskID] = taskIDs(decider.cases)
		}
	}
	for _, group := range dag.groups {
		s.groups = append(s.groups, group.groupID)
		for _, child := range group.children {
			s.children[group.groupID] = append(
				s.children[group.groupID], endpoints("child", child)[0].id(),
			)
		}
	}
	for _, edge := range dag.groupEdges {
		s.groupEdges = append(s.groupEdges, edgeKey{edge.upstream.id(), edge.downstream.id()})
	}
	if len(s.edgeLabels) == 0 {
		s.edgeLabels = nil
	}
	return s
}

// TestAPanicLeavesTheDagAsItWas pins that every check runs before anything is recorded, so a call
// that panics leaves the Dag with what it recorded before the call, in the same order. Each case
// names the check it expects to fail, so that it cannot pass on a check that fails earlier.
func TestAPanicLeavesTheDagAsItWas(t *testing.T) {
	type dagParts struct {
		dag                *DagRef
		transform, checks  *TaskGroupRef
		extracted, cleaned *TaskRef
	}
	for _, tc := range []struct {
		name    string
		prepare func(p dagParts)
		call    func(p dagParts)
		want    string
	}{
		{
			name: "a second TaskGroupSpec",
			call: func(p dagParts) { p.dag.TaskGroup("staging", TaskGroupSpec{}, TaskGroupSpec{}) },
			want: "got 2 airflow.TaskGroupSpec values",
		},
		{
			name: "a group_id that Python rejects",
			call: func(p dagParts) { p.transform.TaskGroup("rows.checks") },
			want: `group_id "rows.checks" holds '.'`,
		},
		{
			name: "a group_id that is taken",
			call: func(p dagParts) { p.transform.TaskGroup("checks") },
			want: `cannot add task group "transform.checks"`,
		},
		{
			name: "a task_id that a group takes",
			call: func(p dagParts) { p.dag.Task(ping, TaskSpec{TaskID: "transform"}) },
			want: `because task group "transform" already takes the ID`,
		},
		{
			name: "a task_id that Python rejects",
			call: func(p dagParts) { p.transform.Task(ping, TaskSpec{TaskID: "clean rows"}) },
			want: `task_id "transform.clean rows" holds ' '`,
		},
		{
			name: "Inputs that do not fit the function",
			call: func(p dagParts) { p.transform.Task(countRows) },
			want: "has 1 parameter(s) after airflow.Context",
		},
		{
			name: "an edge from a group to a task it holds",
			call: func(p dagParts) { p.transform.Before(p.cleaned) },
			want: `task "transform.clean" is inside task group "transform"`,
		},
		{
			name: "a fan-out that relabels a group edge before a later pair fails",
			call: func(p dagParts) {
				p.extracted.Before(Label(p.transform, "relabelled"), p.extracted)
			},
			want: `task "extract" cannot depend on itself`,
		},
		{
			name: "a fan-out that declares a new group edge before a later pair fails",
			call: func(p dagParts) { p.extracted.Before(p.checks, p.extracted) },
			want: `task "extract" cannot depend on itself`,
		},
		{
			name:    "a cycle through a group at Register",
			prepare: func(p dagParts) { p.cleaned.Before(p.extracted) },
			call:    func(p dagParts) { Bundle().Register(p.dag) },
			want:    "contain a cycle",
		},
		{
			name:    "a condition without Then at Register",
			prepare: func(p dagParts) { p.transform.If(isReady) },
			call:    func(p dagParts) { Bundle().Register(p.dag) },
			want:    "has no task from Then",
		},
		{
			name:    "a switch without a case at Register",
			prepare: func(p dagParts) { p.transform.Switch(pickPath) },
			call:    func(p dagParts) { Bundle().Register(p.dag) },
			want:    "has no case",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dag := Dag("etl")
			extracted := orderedTask(t, dag, "extract")
			transform := dag.TaskGroup("transform")
			cleaned := groupTask(t, transform, "clean")
			checks := transform.TaskGroup("checks")
			extracted.Before(Label(transform, "rows"))
			parts := dagParts{dag, transform, checks, extracted, cleaned}
			if tc.prepare != nil {
				tc.prepare(parts)
			}
			before := snapshot(dag)

			assert.Contains(t, panicMessage(t, func() { tc.call(parts) }), tc.want)
			assert.Equal(t, before, snapshot(dag))
		})
	}
}
