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
	"os"
	"os/exec"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Embedding promotes node, so this struct compiles as a Node.
type wrappedNode struct{ Node }

// orderedTask adds a task that passes no data, which is what an order-only edge connects.
func orderedTask(t *testing.T, dag *DagRef, taskID string) *TaskRef {
	t.Helper()
	return dag.Task(ping, TaskSpec{TaskID: taskID})
}

func assertTasks(t *testing.T, got []*TaskRef, want ...*TaskRef) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		assert.Same(t, want[i], got[i], "task %d", i)
	}
}

func assertEdgeLabel(t *testing.T, dag *DagRef, upstream, downstream, want string) {
	t.Helper()
	label, declared := dag.edgeLabels[edgeKey{upstream: upstream, downstream: downstream}]
	require.True(t, declared, "the edge %s -> %s was not declared", upstream, downstream)
	assert.Equal(t, want, label)
}

func TestBeforeDeclaresTheEdgeInBothDirections(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(cleaned)

	assertTasks(t, loaded.downstreams, cleaned)
	assertTasks(t, cleaned.upstreams, loaded)
	assert.Empty(t, loaded.upstreams)
	assert.Empty(t, cleaned.downstreams)
	assertEdgeLabel(t, dag, "load", "cleanup", "")
}

func TestAfterDeclaresTheEdgeBeforeWouldDeclare(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	cleaned.After(loaded)

	assertTasks(t, loaded.downstreams, cleaned)
	assertTasks(t, cleaned.upstreams, loaded)
}

func TestEdgeVerbsFanOut(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	notified := orderedTask(t, dag, "notify")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(notified, cleaned)

	assertTasks(t, loaded.downstreams, notified, cleaned)
	assertTasks(t, notified.upstreams, loaded)
	assertTasks(t, cleaned.upstreams, loaded)
}

// TestBeforeReturnsItsArgumentSet pins what makes a chain work. Before returns the nodes it was
// given, so a.Before(b, c).Before(d) is a >> [b, c] >> d. Returning the receiver would read like
// a chain and mean a second fan-out from a.
func TestBeforeReturnsItsArgumentSet(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	notified := orderedTask(t, dag, "notify")
	cleaned := orderedTask(t, dag, "cleanup")
	done := orderedTask(t, dag, "done")

	extracted.Before(notified, cleaned).Before(done)

	assertTasks(t, extracted.downstreams, notified, cleaned)
	assertTasks(t, done.upstreams, notified, cleaned)
	assertTasks(t, notified.downstreams, done)
	assertTasks(t, cleaned.downstreams, done)
}

func TestAfterReturnsItsArgumentSet(t *testing.T) {
	dag := Dag("etl")
	started := orderedTask(t, dag, "start")
	notified := orderedTask(t, dag, "notify")
	cleaned := orderedTask(t, dag, "cleanup")
	done := orderedTask(t, dag, "done")

	done.After(notified, cleaned).After(started)

	assertTasks(t, done.upstreams, notified, cleaned)
	assertTasks(t, started.downstreams, notified, cleaned)
	assertTasks(t, notified.upstreams, started)
}

func TestEdgeVerbsReturnTheWholeArgumentSetOfEveryNode(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	notified := orderedTask(t, dag, "notify")
	cleaned := orderedTask(t, dag, "cleanup")
	done := orderedTask(t, dag, "done")

	// The set that the first verb returns is one node of the second.
	extracted.Before(extracted.Before(notified, cleaned), done)

	assertTasks(t, extracted.downstreams, notified, cleaned, done)
}

func TestInputsDeclaresAnEdgeInBothDirections(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))

	assertTasks(t, read.downstreams, counted)
	assertTasks(t, counted.upstreams, read)
	assertEdgeLabel(t, dag, "readRows", "countRows", "")
}

func TestInputsDeclaresOneEdgePerTaskItRepeats(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	compared := dag.Task(compareRows, Inputs(read, read))

	assertTasks(t, read.downstreams, compared)
	assertTasks(t, compared.upstreams, read)
}

func TestRedeclaringAnEdgeIsIdempotent(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))

	read.Before(counted)
	counted.After(read)

	assertTasks(t, read.downstreams, counted)
	assertTasks(t, counted.upstreams, read)
}

func TestLabelLabelsTheEdgeToTheNodeItWraps(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	emptyNotice := orderedTask(t, dag, "notify_empty")

	loaded.Before(Label("when empty").On(emptyNotice))

	assertTasks(t, loaded.downstreams, emptyNotice)
	assertEdgeLabel(t, dag, "load", "notify_empty", "when empty")
}

func TestLabelLabelsEachEdgeOfAFanOut(t *testing.T) {
	dag := Dag("etl")
	checked := orderedTask(t, dag, "check")
	processed := orderedTask(t, dag, "process")
	emptyNotice := orderedTask(t, dag, "notify_empty")

	checked.Before(Label("rows found").On(processed), Label("no rows").On(emptyNotice))

	assertEdgeLabel(t, dag, "check", "process", "rows found")
	assertEdgeLabel(t, dag, "check", "notify_empty", "no rows")
}

// TestLabelOnLabelsTheEdgeToEachNodeItIsGiven checks the Go form of Python's
// loaded >> Label("forks") >> [notify_empty, staging, notify_suspicious].
func TestLabelOnLabelsTheEdgeToEachNodeItIsGiven(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	emptyNotice := orderedTask(t, dag, "notify_empty")
	staging := dag.TaskGroup("staging")
	groupTask(t, staging, "stage")
	suspiciousNotice := orderedTask(t, dag, "notify_suspicious")

	loaded.Before(Label("forks").On(emptyNotice, staging, suspiciousNotice))

	assertTasks(t, loaded.downstreams, emptyNotice, suspiciousNotice)
	assertEdgeLabel(t, dag, "load", "notify_empty", "forks")
	assertGroupEdgeLabel(t, dag, "load", "staging", "forks")
	assertEdgeLabel(t, dag, "load", "notify_suspicious", "forks")

	Bundle().Register(dag)
	assertEdgeLabel(t, dag, "load", "staging.stage", "forks")
}

func TestAfterLabelsTheEdgeToTheNodeItWasGiven(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	cleaned := orderedTask(t, dag, "cleanup")

	cleaned.After(Label("always").On(extracted))

	assertEdgeLabel(t, dag, "extract", "cleanup", "always")
}

// TestRedeclaringAnInputsEdgeLabelsIt pins how a data edge gets a label: Inputs takes none, so
// the edge it declared is declared again, which only applies the label.
func TestRedeclaringAnInputsEdgeLabelsIt(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))

	read.Before(Label("rows").On(counted))

	assertTasks(t, read.downstreams, counted)
	assertTasks(t, counted.upstreams, read)
	assertEdgeLabel(t, dag, "readRows", "countRows", "rows")
}

func TestRedeclaringALabelledEdgeKeepsTheLabel(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(Label("always").On(cleaned))
	loaded.Before(cleaned)
	cleaned.After(Label("always").On(loaded))

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
}

// TestASecondLabelOnAnEdgeReplacesTheFirst follows Python, where DAG.set_edge_info overwrites
// rather than merges, so the last label declared on an edge is the one it carries.
func TestASecondLabelOnAnEdgeReplacesTheFirst(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(Label("always").On(cleaned))
	loaded.Before(Label("when empty").On(cleaned))

	assertEdgeLabel(t, dag, "load", "cleanup", "when empty")
	assertTasks(t, loaded.downstreams, cleaned)
}

// TestTheLastLabelOfOneCallWins covers the same rule within a single fan-out, where both labels
// reach the edge through one call.
func TestTheLastLabelOfOneCallWins(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(Label("always").On(cleaned), Label("when empty").On(cleaned))

	assertEdgeLabel(t, dag, "load", "cleanup", "when empty")
	assertTasks(t, loaded.downstreams, cleaned)
}

// TestEdgeVerbsRejectALabelledReceiver covers a verb whose receiver carries a label. That label has
// no edge to go on. The verb panics before it declares any edge. The Java SDK throws in the same
// case. Python's Label("x") >> b drops the label without an error instead. When the label is on a
// zero TaskRef, the verb panics about that TaskRef instead of about the label.
func TestEdgeVerbsRejectALabelledReceiver(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	audited := orderedTask(t, dag, "audit")
	staging := dag.TaskGroup("staging")
	groupTask(t, staging, "stage")

	const fix = "pass airflow.Label(...).On(...) to Before or After as an argument instead"
	for _, tc := range []struct {
		name string
		call func()
		want string
	}{
		{
			name: "Before",
			call: func() { Label("always").On(loaded).Before(cleaned) },
			want: `airflow.Node.Before: Dag "etl": cannot call Before on task "load" ` +
				`labelled "always"; ` + fix,
		},
		{
			name: "After",
			call: func() { Label("always").On(loaded).After(cleaned) },
			want: `airflow.Node.After: Dag "etl": cannot call After on task "load" ` +
				`labelled "always"; ` + fix,
		},
		{
			name: "a verb with no node",
			call: func() { Label("always").On(loaded).Before() },
			want: `airflow.Node.Before: Dag "etl": cannot call Before on task "load" ` +
				`labelled "always"; ` + fix,
		},
		{
			name: "a label on several nodes",
			call: func() { Label("always").On(loaded, staging).Before(audited) },
			want: `airflow.Node.Before: Dag "etl": cannot call Before on task "load" ` +
				`labelled "always", task group "staging" labelled "always"; ` + fix,
		},
		{
			name: "a label on a task that DagRef.Task did not return",
			call: func() { Label("always").On(&TaskRef{}).Before(cleaned) },
			want: "airflow.Node.Before: got a *airflow.TaskRef that DagRef.Task or " +
				"TaskGroupRef.Task did not return",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t, tc.want, tc.call)
		})
	}
	assert.Empty(t, dag.edgeLabels)
	assert.Empty(t, dag.groupEdges)
}

// TestTheOuterLabelOfANestedLabelWins pins what nesting means: the outer Label relabels the
// endpoints the inner one marked, rather than the two combining.
func TestTheOuterLabelOfANestedLabelWins(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(Label("outer").On(Label("inner").On(cleaned)))

	assertEdgeLabel(t, dag, "load", "cleanup", "outer")
}

// TestLabelBelongsToTheVerbItIsPassedTo pins that a label is on the edge of the call it appears
// in. The Node that a verb returns stands for the tasks it pointed at, so it carries no label on
// either side of the next verb: as its receiver, and as one of its nodes.
func TestLabelBelongsToTheVerbItIsPassedTo(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	done := orderedTask(t, dag, "done")
	extracted := orderedTask(t, dag, "extract")

	labelled := loaded.Before(Label("always").On(cleaned))
	labelled.Before(done)
	extracted.Before(labelled)

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
	assertEdgeLabel(t, dag, "cleanup", "done", "")
	assertEdgeLabel(t, dag, "extract", "cleanup", "")
}

// TestALabelledNodeCanGoIntoSeveralVerbs checks that a verb does not change the labels of the Node
// that On returned. The same Node can then label the edges of every verb it is passed to.
func TestALabelledNodeCanGoIntoSeveralVerbs(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	extracted := orderedTask(t, dag, "extract")
	cleaned := orderedTask(t, dag, "cleanup")

	always := Label("always").On(cleaned)
	loaded.Before(always)
	extracted.Before(always)

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
	assertEdgeLabel(t, dag, "extract", "cleanup", "always")
}

// TestAVerbResultCanGoIntoSeveralVerbs checks that a verb copies the Node it is given instead of
// appending to it. The slice behind the Node that a verb returns can have room for more endpoints.
// A verb that appended to that slice would change the Node that another verb returned.
func TestAVerbResultCanGoIntoSeveralVerbs(t *testing.T) {
	dag := Dag("etl")
	source := orderedTask(t, dag, "source")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	audited := orderedTask(t, dag, "audit")
	first := orderedTask(t, dag, "first")
	second := orderedTask(t, dag, "second")
	done := orderedTask(t, dag, "done")

	var none []Node
	// shared stands for extract and load, and its slice has room for one more endpoint.
	shared := source.Before(extracted, loaded, source.Before(none...))
	fromFirst := first.Before(shared, cleaned)
	second.Before(shared, audited)
	fromFirst.Before(done)

	assertTasks(t, done.upstreams, extracted, loaded, cleaned)
}

// TestLabelPassesThroughNoVerbTwice covers the same rule for After, which reads the label from
// the node it was given rather than from its receiver.
func TestLabelPassesThroughNoVerbTwice(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	notified := orderedTask(t, dag, "notify")

	notified.After(cleaned.After(Label("always").On(loaded)))

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
	assertEdgeLabel(t, dag, "load", "notify", "")
}

func TestLabelOnPanicsOnANilNode(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")

	assert.PanicsWithValue(t,
		"airflow.LabelRef.On: node is a nil airflow.Node",
		func() { Label("when empty").On(nil) },
	)
	assert.PanicsWithValue(t,
		"airflow.LabelRef.On: more[1] is a nil airflow.Node",
		func() { Label("when empty").On(loaded, loaded, nil) },
	)
	assert.PanicsWithValue(t,
		"airflow.LabelRef.On: more[0] is a nil *airflow.TaskRef",
		func() { Label("when empty").On(loaded, (*TaskRef)(nil)) },
	)
}

func TestLabelPanicsOnABlankLabel(t *testing.T) {
	for _, text := range []string{"", "   ", "\t\n", " ", "　"} {
		assert.PanicsWithValue(t,
			"airflow.Label: got a blank label; pass the text to put on the edge",
			func() { Label(text) },
			"%q", text,
		)
	}
}

// TestLabelOnPanicsOnALabelThatLabelDidNotReturn covers a nil LabelRef and a zero LabelRef. A zero
// LabelRef has empty text, and an edge verb reads empty text as no label. Without the panic in On,
// the verb would silently ignore that LabelRef.
func TestLabelOnPanicsOnALabelThatLabelDidNotReturn(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")

	for _, tc := range []struct {
		name  string
		label *LabelRef
	}{
		{name: "a nil label", label: nil},
		{name: "a zero label", label: &LabelRef{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.PanicsWithValue(t,
				"airflow.LabelRef.On: airflow.Label did not return the *airflow.LabelRef",
				func() { tc.label.On(loaded) },
			)
		})
	}
}

func TestEdgeVerbsRejectANilNode(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	assert.PanicsWithValue(t,
		"airflow.Node.Before: nodes[1] is a nil airflow.Node",
		func() { loaded.Before(cleaned, nil) },
	)
	assert.PanicsWithValue(t,
		"airflow.Node.After: nodes[0] is a nil airflow.Node",
		func() { loaded.After(nil) },
	)
	assert.PanicsWithValue(t,
		"airflow.Node.Before: nodes[1] is a nil *airflow.TaskRef",
		func() { loaded.Before(cleaned, (*TaskRef)(nil)) },
	)
}

func TestEdgeVerbsRejectATaskThatTaskDidNotReturn(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	copied := *loaded

	assert.PanicsWithValue(t,
		"airflow.Node.Before: got a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task "+
			"did not return",
		func() { (&TaskRef{}).Before(loaded) },
	)
	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl" got a *airflow.TaskRef that DagRef.Task or `+
			`TaskGroupRef.Task did not return`,
		func() { loaded.Before(&copied) },
	)
}

func TestEdgeVerbsRejectATaskOfAnotherDag(t *testing.T) {
	loaded := orderedTask(t, Dag("etl"), "load")
	cleaned := orderedTask(t, Dag("reporting"), "cleanup")

	assert.PanicsWithValue(t,
		`airflow.Node.Before: cannot declare an edge between task "load" of Dag "etl" and `+
			`task "cleanup" of Dag "reporting"; an edge connects the tasks and task groups of one Dag`,
		func() { loaded.Before(cleaned) },
	)
}

func TestEdgeVerbsRejectATaskBeforeItself(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")

	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": task "load" cannot depend on itself`,
		func() { loaded.Before(loaded) },
	)
	assert.Empty(t, loaded.downstreams)
}

// TestEdgeVerbsRecordNoEdgeWhenAPairIsRejected pins that the checks run over the whole fan-out
// before any of it is recorded, the way DagRef.Task settles every check before it writes a task.
func TestEdgeVerbsRecordNoEdgeWhenAPairIsRejected(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	notified := orderedTask(t, dag, "notify")

	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": task "load" cannot depend on itself`,
		func() { loaded.Before(cleaned, loaded) },
	)
	assert.PanicsWithValue(t,
		`airflow.Node.After: Dag "etl": task "load" cannot depend on itself`,
		func() { loaded.After(notified, loaded) },
	)

	assert.Empty(t, loaded.downstreams)
	assert.Empty(t, loaded.upstreams)
	assert.Empty(t, cleaned.upstreams)
	assert.Empty(t, notified.downstreams)
	assert.Empty(t, dag.edgeLabels)
}

func TestEdgeVerbsAfterRegisterPanic(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	Bundle().Register(dag)

	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl" has already been registered; `+
			`declare every edge before Register`,
		func() { loaded.Before(cleaned) },
	)
	assert.Empty(t, loaded.downstreams)
}

func TestEdgeVerbsWithNoNodeDeclareNoEdge(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	// A spread of an empty slice reaches a verb with no node, and the empty set it returns
	// carries a chain no further.
	var none []Node
	loaded.Before(none...).Before(cleaned)
	loaded.After(none...)
	// When On is given only that empty set, it returns a Node that holds no task or task group, so
	// nothing carries the label. A verb called on that Node then has no label to reject, so it does
	// not panic.
	Label("always").On(loaded.Before(none...)).Before(cleaned)

	assert.Empty(t, loaded.downstreams)
	assert.Empty(t, loaded.upstreams)
	assert.Empty(t, cleaned.upstreams)
	assert.Empty(t, dag.edgeLabels)
}

// TestAVerbWithAnEmptyReceiverRejectsALabel covers a verb whose receiver stands for no task or task
// group, such as the Node that a verb given no node returns. The verb declares no edge, so a label
// that it is given has no edge to go on.
func TestAVerbWithAnEmptyReceiverRejectsALabel(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	done := orderedTask(t, dag, "done")

	var none []Node
	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": the receiver stands for no task or task group, `+
			`so there is no edge for task "cleanup" labelled "always"`,
		func() { loaded.Before(none...).Before(Label("always").On(cleaned)) },
	)
	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": the receiver stands for no task or task group, `+
			`so there is no edge for task "done" labelled "always"`,
		func() { loaded.Before(none...).Before(cleaned, Label("always").On(done)) },
	)
	assert.Empty(t, cleaned.upstreams)
	assert.Empty(t, done.upstreams)
	assert.Empty(t, dag.edgeLabels)
}

// TestEdgeVerbsCheckTheTasksEvenWithNoNode pins that a verb with no node to point at still checks
// the tasks it was called on. It declares no edge, but a Dag that can no longer take one, or a
// task the Dag never returned, is a mistake either way.
func TestEdgeVerbsCheckTheTasksEvenWithNoNode(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	var none []Node

	assert.PanicsWithValue(t,
		"airflow.Node.Before: got a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task "+
			"did not return",
		func() { (&TaskRef{}).Before(none...) },
	)
	assert.PanicsWithValue(t,
		"airflow.Node.After: the receiver is a nil *airflow.TaskRef",
		func() { (*TaskRef)(nil).After(none...) },
	)

	Bundle().Register(dag)
	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl" has already been registered; `+
			`declare every edge before Register`,
		func() { loaded.Before(none...) },
	)
}

func TestEdgeVerbsAreSafeForConcurrentUse(t *testing.T) {
	const workers = 8

	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	tasks := make([]*TaskRef, workers)
	for worker := range workers {
		tasks[worker] = orderedTask(t, dag, fmt.Sprintf("notify_%d", worker))
	}

	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			loaded.Before(tasks[worker])
			tasks[worker].After(loaded)
		}()
	}
	wg.Wait()

	assert.Len(t, loaded.downstreams, workers)
	assert.Len(t, dag.edgeLabels, workers)
}

// TestEdgeVerbsRejectANodeTheyDoNotDefine covers the one gap in the seal: embedding promotes the
// node method, so a struct that embeds a Node compiles as one.
func TestEdgeVerbsRejectANodeTheyDoNotDefine(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	assert.PanicsWithValue(t,
		"airflow.Node.Before: nodes[0] has type airflow.wrappedNode, "+
			"which is not a node that package airflow defines",
		func() { loaded.Before(wrappedNode{cleaned}) },
	)
	assert.PanicsWithValue(t,
		"airflow.LabelRef.On: node has type airflow.wrappedNode, "+
			"which is not a node that package airflow defines",
		func() { Label("when empty").On(wrappedNode{cleaned}) },
	)
	assert.PanicsWithValue(t,
		"airflow.LabelRef.On: more[0] has type airflow.wrappedNode, "+
			"which is not a node that package airflow defines",
		func() { Label("when empty").On(cleaned, wrappedNode{cleaned}) },
	)
	assert.Empty(t, loaded.downstreams)
}

func TestNodeIsSealed(t *testing.T) {
	typ := reflect.TypeFor[Node]()
	require.Equal(t, 3, typ.NumMethod())
	// reflect reports a package path only for an unexported method.
	method, ok := typ.MethodByName("node")
	require.True(t, ok, "Node has no unexported node method to seal it")
	assert.Equal(t, "github.com/apache/airflow/go-sdk/airflow", method.PkgPath)
}

func TestNodeRejectsForeignTypes(t *testing.T) {
	if testing.Short() {
		t.Skip("shells out to `go build`")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("go toolchain not on PATH")
	}

	out, err := exec.Command("go", "build", "-o", os.DevNull, "./testdata/foreignnode").
		CombinedOutput()

	require.Error(t, err, "a type defined outside package airflow must not compile as a Node")
	// The test checks only the two type names, so that a change in the wording of the compiler
	// error does not break it.
	assert.Contains(t, string(out), "foreignNode")
	assert.Contains(t, string(out), "airflow.Node")
}
