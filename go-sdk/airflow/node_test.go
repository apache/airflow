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

	loaded.Before(Label(emptyNotice, "when empty"))

	assertTasks(t, loaded.downstreams, emptyNotice)
	assertEdgeLabel(t, dag, "load", "notify_empty", "when empty")
}

func TestLabelLabelsEachEdgeOfAFanOut(t *testing.T) {
	dag := Dag("etl")
	checked := orderedTask(t, dag, "check")
	processed := orderedTask(t, dag, "process")
	emptyNotice := orderedTask(t, dag, "notify_empty")

	checked.Before(Label(processed, "rows found"), Label(emptyNotice, "no rows"))

	assertEdgeLabel(t, dag, "check", "process", "rows found")
	assertEdgeLabel(t, dag, "check", "notify_empty", "no rows")
}

func TestAfterLabelsTheEdgeToTheNodeItWasGiven(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	cleaned := orderedTask(t, dag, "cleanup")

	cleaned.After(Label(extracted, "always"))

	assertEdgeLabel(t, dag, "extract", "cleanup", "always")
}

// TestRedeclaringAnInputsEdgeLabelsIt pins how a data edge gets a label: Inputs takes none, so
// the edge it declared is declared again, which only applies the label.
func TestRedeclaringAnInputsEdgeLabelsIt(t *testing.T) {
	dag := Dag("etl")
	read := dag.Task(readRows)
	counted := dag.Task(countRows, Inputs(read))

	read.Before(Label(counted, "rows"))

	assertTasks(t, read.downstreams, counted)
	assertTasks(t, counted.upstreams, read)
	assertEdgeLabel(t, dag, "readRows", "countRows", "rows")
}

func TestRedeclaringALabelledEdgeKeepsTheLabel(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")

	loaded.Before(Label(cleaned, "always"))
	loaded.Before(cleaned)
	cleaned.After(Label(loaded, "always"))

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
}

func TestEdgeRejectsASecondLabel(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	loaded.Before(Label(cleaned, "always"))

	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": the edge from task "load" to task "cleanup" is already `+
			`labelled "always", so it cannot also be labelled "when empty"; label an edge once`,
		func() { loaded.Before(Label(cleaned, "when empty")) },
	)
	assertEdgeLabel(t, dag, "load", "cleanup", "always")
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

	labelled := loaded.Before(Label(cleaned, "always"))
	labelled.Before(done)
	extracted.Before(labelled)

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
	assertEdgeLabel(t, dag, "cleanup", "done", "")
	assertEdgeLabel(t, dag, "extract", "cleanup", "")
}

// TestLabelPassesThroughNoVerbTwice covers the same rule for After, which reads the label from
// the node it was given rather than from its receiver.
func TestLabelPassesThroughNoVerbTwice(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	notified := orderedTask(t, dag, "notify")

	notified.After(cleaned.After(Label(loaded, "always")))

	assertEdgeLabel(t, dag, "load", "cleanup", "always")
	assertEdgeLabel(t, dag, "load", "notify", "")
}

func TestLabelPanicsOnANilNode(t *testing.T) {
	assert.PanicsWithValue(t,
		"airflow.Label: got a nil airflow.Node",
		func() { Label(nil, "when empty") },
	)
}

func TestLabelPanicsOnAnEmptyLabel(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")

	assert.PanicsWithValue(t,
		"airflow.Label: got an empty label; pass the text to put on the edge",
		func() { Label(loaded, "") },
	)
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
		"airflow.Node.Before: got a nil *airflow.TaskRef",
		func() { loaded.Before((*TaskRef)(nil)) },
	)
}

func TestEdgeVerbsRejectATaskThatTaskDidNotReturn(t *testing.T) {
	dag := Dag("etl")
	loaded := orderedTask(t, dag, "load")
	copied := *loaded

	assert.PanicsWithValue(t,
		"airflow.Node.Before: got a *airflow.TaskRef that DagRef.Task did not return",
		func() { (&TaskRef{}).Before(loaded) },
	)
	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl" got a *airflow.TaskRef that DagRef.Task did not return`,
		func() { loaded.Before(&copied) },
	)
}

func TestEdgeVerbsRejectATaskOfAnotherDag(t *testing.T) {
	loaded := orderedTask(t, Dag("etl"), "load")
	cleaned := orderedTask(t, Dag("reporting"), "cleanup")

	assert.PanicsWithValue(t,
		`airflow.Node.Before: cannot declare an edge between task "load" of Dag "etl" and `+
			`task "cleanup" of Dag "reporting"; an edge connects tasks of one Dag`,
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
		`airflow.Node.Before: Dag "etl": the edge from task "load" to task "cleanup" is already `+
			`labelled "always", so it cannot also be labelled "when empty"`+
			`; label an edge once`,
		func() { loaded.Before(Label(cleaned, "always"), Label(cleaned, "when empty")) },
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

// TestACycleCanLeaveTheEarlierEdgesOfTheCall pins the one check that cannot run up front: whether
// an edge closes a cycle depends on the edges the same call already recorded.
func TestACycleCanLeaveTheEarlierEdgesOfTheCall(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	notified := orderedTask(t, dag, "notify")
	extracted.Before(loaded)

	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": an edge from task "load" to task "extract" would `+
			`close a cycle: extract -> load -> extract`,
		func() { loaded.Before(notified, extracted) },
	)
	// load -> notify came first and stands; load -> extract is the pair that closed the cycle.
	assertTasks(t, loaded.downstreams, notified)
	assertTasks(t, extracted.downstreams, loaded)
}

func TestEdgeVerbsRejectACycle(t *testing.T) {
	dag := Dag("etl")
	extracted := orderedTask(t, dag, "extract")
	loaded := orderedTask(t, dag, "load")
	cleaned := orderedTask(t, dag, "cleanup")
	extracted.Before(loaded).Before(cleaned)

	assert.PanicsWithValue(t,
		`airflow.Node.Before: Dag "etl": an edge from task "cleanup" to task "extract" would `+
			`close a cycle: extract -> load -> cleanup -> extract`,
		func() { cleaned.Before(extracted) },
	)
	assert.PanicsWithValue(t,
		`airflow.Node.After: Dag "etl": an edge from task "cleanup" to task "extract" would `+
			`close a cycle: extract -> load -> cleanup -> extract`,
		func() { extracted.After(cleaned) },
	)
	assertTasks(t, cleaned.downstreams)
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

	assert.Empty(t, loaded.downstreams)
	assert.Empty(t, loaded.upstreams)
	assert.Empty(t, cleaned.upstreams)
	assert.Empty(t, dag.edgeLabels)
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
		"airflow.Label: node has type airflow.wrappedNode, "+
			"which is not a node that package airflow defines",
		func() { Label(wrappedNode{cleaned}, "when empty") },
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
