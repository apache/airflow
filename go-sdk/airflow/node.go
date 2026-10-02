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
	"slices"
	"strings"
)

// Node is what an edge between tasks connects. A [TaskRef] is one, so the task that
// [DagRef.Task] returns is an edge endpoint.
//
// Before and After declare the order-only edges that Python writes with >> and <<, for tasks
// that have to run in an order but pass no data. [Inputs] declares the edge that carries a
// result.
//
// Both verbs are variadic, so one call fans out, and both return their argument set as one Node
// rather than the receiver. That is what makes a chain run the way it reads:
//
//	loaded.Before(notified, cleaned)       // loaded >> [notify, cleanup]
//	cleaned.After(extracted)               // cleanup << extracted
//	extracted.Before(loaded).Before(done)  // extract >> load >> done
//
// The second Before above starts from load, not from extract.
//
// Node is the Go counterpart of Python's DAGNode, where set_upstream and set_downstream live.
// Its node method is unexported, so a type declared outside package airflow cannot be a Node.
type Node interface {
	// Before makes the receiver an upstream task of every node, as Python's >> does.
	Before(nodes ...Node) Node
	// After makes the receiver a downstream task of every node, as Python's << does.
	After(nodes ...Node) Node
	// node seals the interface. Only a type that package airflow declares is a Node, so
	// endpoints can read what one stands for.
	node()
}

// nodeEndpoint is one task that a Node stands for, with the label that [Label] carries into the
// edge verb the Node is passed to.
type nodeEndpoint struct {
	task  *TaskRef
	label string
}

// nodeSet is the argument set that Before and After return as one Node, and the Node that
// [Label] returns. A label belongs to the edge of the verb the Node is passed to, so a nodeSet
// carries its labels no further once it is the receiver of the next verb.
type nodeSet []nodeEndpoint

func (nodeSet) node() {}

// unlabelled returns the set with its labels dropped. A label belongs to the one verb it was
// passed to, so the Node a verb returns, which stands for the tasks it pointed at, carries none.
func (s nodeSet) unlabelled() nodeSet {
	plain := make(nodeSet, len(s))
	for i, endpoint := range s {
		plain[i] = nodeEndpoint{task: endpoint.task}
	}
	return plain
}

func (s nodeSet) Before(nodes ...Node) Node { return declareEdges(s, nodes, dirBefore) }

func (s nodeSet) After(nodes ...Node) Node { return declareEdges(s, nodes, dirAfter) }

func (*TaskRef) node() {}

// endpoints returns the tasks that node stands for. The Node method is unexported, so the types
// of this package are the only Nodes, and a struct that embeds one is all that reaches the
// default case. where names the Node in a panic, such as "airflow.Node.Before: nodes[0]".
func endpoints(where string, node Node) []nodeEndpoint {
	switch node := node.(type) {
	case *TaskRef:
		return []nodeEndpoint{{task: node}}
	case nodeSet:
		return node
	default:
		panic(fmt.Sprintf(
			"%s has type %T, which is not a node that package airflow defines", where, node,
		))
	}
}

// Before makes the task an upstream task of every node, which is Python's
// loaded >> [notify, cleanup]:
//
//	loaded.Before(notified, cleaned)
//
// The edge carries no data, so a task that only comes after another takes no parameter for it.
// Use [Inputs] for an edge that passes a result.
//
// Before returns the nodes it was given as one Node, not the receiver, so a chain fans out from
// them: loaded.Before(notified, cleaned).Before(done) is loaded >> [notify, cleanup] >> done.
// Wrap a node in [Label] to label the edge that reaches it.
//
// Declaring an edge that the Dag already has changes nothing, other than to apply a [Label].
//
// Before panics if:
//   - a node is nil, or is a task that [DagRef.Task] did not return
//   - a node belongs to another Dag
//   - the edge would make a task depend on itself, or would close a cycle
//   - the Dag is already registered
//
// Every check but the cycle runs before the call records any edge, so a fan-out that panics for
// one of them leaves the Dag as it was. Whether an edge closes a cycle depends on the edges
// recorded before it, so a cycle panic can leave the earlier edges of the same call behind.
func (t *TaskRef) Before(nodes ...Node) Node { return declareEdges(t, nodes, dirBefore) }

// After makes the task a downstream task of every node, which is Python's cleanup << extracted:
//
//	cleaned.After(extracted)
//
// It is Before with the direction reversed, and it panics for the same reasons. Like Before, it
// returns the nodes it was given as one Node, so cleaned.After(extracted).After(started) is
// cleanup << extract << start.
func (t *TaskRef) After(nodes ...Node) Node { return declareEdges(t, nodes, dirAfter) }

// Label puts text on the edge that an edge verb declares to node, as Python's
// loaded >> Label("when empty") >> notify_empty does:
//
//	loaded.Before(Label(emptyNotice, "when empty"))
//
// The label wraps the endpoint rather than the call, so each edge of a fan-out can carry a label
// of its own:
//
//	checked.Before(Label(processed, "rows found"), Label(emptyNotice, "no rows"))
//
// A label is the Node's only in the verb it is passed to. The Node that Label returns stands for
// node itself, and the Node a verb returns carries no label on either side of the next verb. So a
// label on the receiver of a verb has no edge to land on and is dropped, the way Python's
// Label("x") >> b alone sets no label.
//
// A label declared on an edge that already carries one replaces it, as Python's DAG.set_edge_info
// does.
//
// An [Inputs] edge is labelled by declaring it again, which is idempotent:
//
//	transformed := dag.Task(transform, Inputs(extracted))
//	extracted.Before(Label(transformed, "rows"))
//
// Label panics if node is nil or text is empty.
func Label(node Node, text string) Node {
	if node == nil {
		panic("airflow.Label: got a nil airflow.Node")
	}
	if text == "" {
		panic("airflow.Label: got an empty label; pass the text to put on the edge")
	}
	labelling := endpoints("airflow.Label: node", node)
	labelled := make(nodeSet, len(labelling))
	for i, endpoint := range labelling {
		labelled[i] = nodeEndpoint{task: endpoint.task, label: text}
	}
	return labelled
}

// edgeKey identifies an edge of a Dag. The task_ids of a Dag are unique, so they name the ends.
type edgeKey struct{ upstream, downstream string }

// edgeDir is which way an edge verb points: [TaskRef.Before] from its receiver, and
// [TaskRef.After] at it.
type edgeDir int

const (
	dirBefore edgeDir = iota
	dirAfter
)

func (dir edgeDir) String() string {
	if dir == dirAfter {
		return "After"
	}
	return "Before"
}

// order returns the two ends of the edge between an endpoint of the receiver and one of the nodes
// the verb was given.
func (dir edgeDir) order(recv, arg *TaskRef) (upstream, downstream *TaskRef) {
	if dir == dirAfter {
		return arg, recv
	}
	return recv, arg
}

// pendingEdge is an edge that declareEdges has checked and is about to record.
type pendingEdge struct {
	upstream, downstream *TaskRef
	key                  edgeKey
	label                string
}

// declareEdges records an edge from every endpoint of receiver to every node it was given, or
// the other way round for After, and returns those nodes as one Node.
func declareEdges(receiver Node, nodes []Node, dir edgeDir) Node {
	where := "airflow.Node." + dir.String()
	args := make(nodeSet, 0, len(nodes))
	for i, node := range nodes {
		if node == nil {
			panic(fmt.Sprintf("%s: nodes[%d] is a nil airflow.Node", where, i))
		}
		args = append(args, endpoints(fmt.Sprintf("%s: nodes[%d]", where, i), node)...)
	}
	ends := endpoints(where+": the receiver", receiver)
	all := slices.Concat(ends, args)
	if len(all) == 0 {
		return nodeSet(nil)
	}

	dag := edgeDag(where, all)

	dag.mu.Lock()
	defer dag.mu.Unlock()

	if dag.registered {
		panic(fmt.Sprintf(
			"%s: Dag %q has already been registered; declare every edge before Register",
			where, dag.dagID,
		))
	}
	for _, endpoint := range all {
		// A zero TaskRef and a copy of a TaskRef get here.
		if dag.tasksByID[endpoint.task.taskID] != endpoint.task {
			panic(fmt.Sprintf(
				"%s: Dag %q got a *airflow.TaskRef that DagRef.Task did not return",
				where, dag.dagID,
			))
		}
	}
	// A verb with no node to point at, which a spread of an empty slice reaches, declares no
	// edge. So does one on the empty set that such a verb returned.
	if len(ends) == 0 || len(args) == 0 {
		return args.unlabelled()
	}

	// Check and merge every pair the call declares before it records any of them, the way
	// DagRef.Task settles every check before it writes the task. A cycle is the one exception:
	// whether an edge closes one depends on the edges recorded before it, so addEdgeLocked
	// raises that as it records.
	pending := make([]pendingEdge, 0, len(ends)*len(args))
	at := make(map[edgeKey]int, len(ends)*len(args))
	for _, end := range ends {
		for _, arg := range args {
			upstream, downstream := dir.order(end.task, arg.task)
			if upstream == downstream {
				panic(fmt.Sprintf(
					"%s: Dag %q: task %q cannot depend on itself",
					where, dag.dagID, upstream.taskID,
				))
			}
			key := edgeKey{upstream: upstream.taskID, downstream: downstream.taskID}
			i, declared := at[key]
			if !declared {
				i = len(pending)
				at[key] = i
				pending = append(pending, pendingEdge{
					upstream: upstream, downstream: downstream, key: key,
					label: dag.edgeLabels[key],
				})
			}
			// The label belongs to the node the verb was given, in either direction.
			pending[i].label = mergeLabel(pending[i].label, arg.label)
		}
	}
	for _, edge := range pending {
		dag.addEdgeLocked(edge.upstream, edge.downstream, edge.label, where)
	}
	return args.unlabelled()
}

// mergeLabel returns the label an edge carries once label is declared on it. A declaration that
// carries no label leaves the edge's own label alone, which is what makes redeclaring an edge
// idempotent, and one that carries a label overwrites it, as Python's DAG.set_edge_info does.
func mergeLabel(declared, label string) string {
	if label == "" {
		return declared
	}
	return label
}

// edgeDag returns the Dag that every end of an edge belongs to. It panics unless each end is a
// task of that one Dag.
func edgeDag(where string, ends []nodeEndpoint) *DagRef {
	var first *TaskRef
	for _, end := range ends {
		task := end.task
		switch {
		case task == nil:
			panic(fmt.Sprintf("%s: got a nil *airflow.TaskRef", where))
		case task.dag == nil:
			panic(fmt.Sprintf(
				"%s: got a *airflow.TaskRef that DagRef.Task did not return", where,
			))
		case first == nil:
			first = task
		case task.dag != first.dag:
			panic(fmt.Sprintf(
				"%s: cannot declare an edge between task %q of Dag %q and task %q of Dag %q; "+
					"an edge connects tasks of one Dag",
				where, first.taskID, first.dag.dagID, task.taskID, task.dag.dagID,
			))
		}
	}
	return first.dag
}

// addEdgeLocked records one edge of d, which the caller holds d.mu for. An edge d already has is
// recorded once, and label is what it carries from here on, so a caller that declares an edge
// again settles the label with [mergeLabel] first. The cycle is the one thing addEdgeLocked
// checks, because whether an edge closes one depends on the edges already recorded.
func (d *DagRef) addEdgeLocked(upstream, downstream *TaskRef, label, where string) {
	if d.edgeLabels == nil {
		d.edgeLabels = make(map[edgeKey]string)
	}
	key := edgeKey{upstream: upstream.taskID, downstream: downstream.taskID}
	if _, exists := d.edgeLabels[key]; !exists {
		if cycle := d.pathLocked(downstream, upstream); cycle != nil {
			panic(fmt.Sprintf(
				"%s: Dag %q: an edge from task %q to task %q would close a cycle: %s",
				where, d.dagID, upstream.taskID, downstream.taskID,
				strings.Join(append(cycle, downstream.taskID), " -> "),
			))
		}
		upstream.downstreams = append(upstream.downstreams, downstream)
		downstream.upstreams = append(downstream.upstreams, upstream)
	}
	d.edgeLabels[key] = label
}

// pathLocked returns the task_ids on a path from task from to task to, following the edges
// downstream, and nil when there is none. The caller holds d.mu.
func (d *DagRef) pathLocked(from, to *TaskRef) []string {
	visited := map[*TaskRef]bool{from: true}
	var walk func(task *TaskRef, path []string) []string
	walk = func(task *TaskRef, path []string) []string {
		path = append(path, task.taskID)
		if task == to {
			return slices.Clone(path)
		}
		for _, downstream := range task.downstreams {
			if visited[downstream] {
				continue
			}
			visited[downstream] = true
			if found := walk(downstream, path); found != nil {
				return found
			}
		}
		return nil
	}
	return walk(from, nil)
}
