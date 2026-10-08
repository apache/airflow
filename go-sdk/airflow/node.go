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
)

// Node is what an edge connects: a task or a whole task group. A [TaskRef] and a [TaskGroupRef]
// are both Nodes, so the task that [DagRef.Task] returns and the group that [DagRef.TaskGroup]
// returns are both edge endpoints.
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
// Node is the Go counterpart of Python's DAGNode, where set_upstream and set_downstream live. In
// Python, both an operator and a TaskGroup are DAGNodes.
// Its node method is unexported, so a type declared outside package airflow can be a Node only by
// embedding one, and Before and After reject such a type as an argument.
type Node interface {
	// Before makes the receiver an upstream of every node, as Python's >> does.
	Before(nodes ...Node) Node
	// After makes the receiver a downstream of every node, as Python's << does.
	After(nodes ...Node) Node
	// node seals the interface. Only a type that package airflow declares is a Node, so
	// endpoints can read what one stands for.
	node()
}

// nodeEndpoint is one task or one task group that a Node stands for, with the label that [Label]
// carries into the edge verb the Node is passed to. Exactly one of task and group is set, except
// for the endpoint of a nil *TaskRef, which has neither.
type nodeEndpoint struct {
	task  *TaskRef
	group *TaskGroupRef
	label string
}

// id returns the task_id or the group_id of e. Tasks and task groups share one namespace of IDs,
// so the ID names one node of the Dag.
func (e nodeEndpoint) id() string {
	if e.group != nil {
		return e.group.groupID
	}
	return e.task.taskID
}

// owner returns the Dag that e belongs to, and nil for an endpoint that no Dag returned.
func (e nodeEndpoint) owner() *DagRef {
	if e.group != nil {
		return e.group.dag
	}
	return e.task.dag
}

// container returns the innermost task group that holds e, and nil when only the Dag does.
func (e nodeEndpoint) container() *TaskGroupRef {
	if e.group != nil {
		return e.group.parent
	}
	return e.task.group
}

// describe names e in a panic message, such as task "load" or task group "transform".
func (e nodeEndpoint) describe() string {
	if e.group != nil {
		return fmt.Sprintf("task group %q", e.group.groupID)
	}
	return fmt.Sprintf("task %q", e.task.taskID)
}

// nodeSet is the argument set that Before and After return as one Node, and the Node that
// [Label] returns. A label belongs to the edge of the verb the Node is passed to, so a nodeSet
// carries its labels no further once it is the receiver of the next verb.
type nodeSet []nodeEndpoint

func (nodeSet) node() {}

// unlabelled returns the set with its labels dropped. A label belongs to the one verb it was
// passed to, so the Node a verb returns, which stands for the nodes it pointed at, carries none.
func (s nodeSet) unlabelled() nodeSet {
	plain := make(nodeSet, len(s))
	for i, endpoint := range s {
		plain[i] = nodeEndpoint{task: endpoint.task, group: endpoint.group}
	}
	return plain
}

func (s nodeSet) Before(nodes ...Node) Node { return declareEdges(s, nodes, dirBefore) }

func (s nodeSet) After(nodes ...Node) Node { return declareEdges(s, nodes, dirAfter) }

func (*TaskRef) node() {}

// endpoints returns the tasks and task groups that node stands for. The Node method is
// unexported, so the types of this package are the only Nodes, and a struct that embeds one is all
// that reaches the default case. where names the Node in a panic, such as
// "airflow.Node.Before: nodes[0]".
func endpoints(where string, node Node) []nodeEndpoint {
	switch node := node.(type) {
	case *TaskRef:
		return []nodeEndpoint{{task: node}}
	case *TaskGroupRef:
		if node == nil {
			panic(where + " is a nil *airflow.TaskGroupRef")
		}
		return []nodeEndpoint{{group: node}}
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
// A node can also be a task group, which [TaskGroupRef.Before] describes.
//
// Before panics if:
//   - a node is nil, is a task that [DagRef.Task] or [TaskGroupRef.Task] did not return, or is a
//     task group that [DagRef.TaskGroup] or [TaskGroupRef.TaskGroup] did not return
//   - a node belongs to another Dag
//   - the edge would make a task depend on itself
//   - a node is a task group that holds the task
//   - the Dag is already registered
//
// Every check runs before the call records any edge, so a fan-out that panics leaves the Dag as
// it was. Edges that close a cycle between other tasks are [BundleRef.Register]'s to reject,
// over the whole graph at once.
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
// A label on an edge to or from a task group stays on that edge, as [TaskGroupRef.Before]
// describes. A label on an edge between two tasks stays on that edge too, even when the tasks are
// in different task groups. In that case Python can replace the receiver of the verb with a group
// that holds it, which makes the edge connect other tasks.
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
		labelled[i] = nodeEndpoint{task: endpoint.task, group: endpoint.group, label: text}
	}
	return labelled
}

// edgeKey identifies an edge of a Dag. Tasks and task groups share one namespace of IDs, so a
// task_id or a group_id names each end.
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
func (dir edgeDir) order(recv, arg nodeEndpoint) (upstream, downstream nodeEndpoint) {
	if dir == dirAfter {
		return arg, recv
	}
	return recv, arg
}

// pendingEdge is an edge that declareEdges has checked and is about to record.
type pendingEdge struct {
	upstream, downstream nodeEndpoint
	key                  edgeKey
	label                string
}

// betweenTasks reports whether both ends of e are tasks rather than task groups.
func (e pendingEdge) betweenTasks() bool {
	return e.upstream.group == nil && e.downstream.group == nil
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
		if endpoint.group == nil && dag.tasksByID[endpoint.task.taskID] != endpoint.task {
			panic(fmt.Sprintf(
				"%s: Dag %q got a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task "+
					"did not return",
				where, dag.dagID,
			))
		}
		dag.checkGroupLocked(where, endpoint.group)
	}
	// A verb with no node to point at, which a spread of an empty slice reaches, declares no
	// edge. So does one on the empty set that such a verb returned.
	if len(ends) == 0 || len(args) == 0 {
		return args.unlabelled()
	}

	// Check and merge every pair the call declares before it records any of them.
	pending := make([]pendingEdge, 0, len(ends)*len(args))
	at := make(map[edgeKey]int, len(ends)*len(args))
	for _, end := range ends {
		for _, arg := range args {
			upstream, downstream := dir.order(end, arg)
			if upstream.task == downstream.task && upstream.group == downstream.group {
				panic(fmt.Sprintf(
					"%s: Dag %q: %s cannot depend on itself", where, dag.dagID, upstream.describe(),
				))
			}
			checkGroupEdgeEnds(where, dag, upstream, downstream)
			key := edgeKey{upstream: upstream.id(), downstream: downstream.id()}
			i, declared := at[key]
			if !declared {
				i = len(pending)
				at[key] = i
				edge := pendingEdge{upstream: upstream, downstream: downstream, key: key}
				if edge.betweenTasks() {
					edge.label = dag.edgeLabels[key]
				} else {
					edge.label = dag.groupEdgeLabels[key]
				}
				pending = append(pending, edge)
			}
			// The label belongs to the node the verb was given, in either direction.
			pending[i].label = mergeLabel(pending[i].label, arg.label)
		}
	}
	for _, edge := range pending {
		if edge.betweenTasks() {
			dag.addEdgeLocked(edge.upstream.task, edge.downstream.task, edge.label)
		} else {
			dag.addGroupEdgeLocked(edge.upstream, edge.downstream, edge.label)
		}
	}
	return args.unlabelled()
}

// checkGroupEdgeEnds panics if one end of an edge is a task group that holds the other end. The
// edge would order the group against part of itself: in Python, group >> task_in_group makes the
// last tasks of the group upstreams of a task that may be one of them.
func checkGroupEdgeEnds(where string, dag *DagRef, upstream, downstream nodeEndpoint) {
	for _, pair := range [][2]nodeEndpoint{{upstream, downstream}, {downstream, upstream}} {
		if group := pair[0].group; group != nil && group.holds(pair[1]) {
			panic(fmt.Sprintf(
				"%s: Dag %q: %s is inside task group %q, so an edge cannot connect them; "+
					"an edge connects a group to a task or a group outside it",
				where, dag.dagID, pair[1].describe(), group.groupID,
			))
		}
	}
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
// task or a task group of that one Dag.
func edgeDag(where string, ends []nodeEndpoint) *DagRef {
	var first nodeEndpoint
	var dag *DagRef
	for _, end := range ends {
		switch {
		case end.task == nil && end.group == nil:
			panic(fmt.Sprintf("%s: got a nil *airflow.TaskRef", where))
		case end.group != nil && end.group.dag == nil:
			panic(fmt.Sprintf(
				"%s: got a *airflow.TaskGroupRef that DagRef.TaskGroup or "+
					"TaskGroupRef.TaskGroup did not return", where,
			))
		case end.group == nil && end.task.dag == nil:
			panic(fmt.Sprintf(
				"%s: got a *airflow.TaskRef that DagRef.Task or TaskGroupRef.Task did not return",
				where,
			))
		case dag == nil:
			first, dag = end, end.owner()
		case end.owner() != dag:
			panic(fmt.Sprintf(
				"%s: cannot declare an edge between %s of Dag %q and %s of Dag %q; "+
					"an edge connects the tasks and task groups of one Dag",
				where, first.describe(), dag.dagID, end.describe(), end.owner().dagID,
			))
		}
	}
	return dag
}

// addEdgeLocked records one edge of d, which the caller holds d.mu for. An edge d already has is
// recorded once, and label is what it carries from here on, so a caller that declares an edge
// again settles the label with [mergeLabel] first. Whether the edges of a Dag close a cycle is
// [BundleRef.Register]'s to answer, over the whole graph at once.
func (d *DagRef) addEdgeLocked(upstream, downstream *TaskRef, label string) {
	if d.edgeLabels == nil {
		d.edgeLabels = make(map[edgeKey]string)
	}
	key := edgeKey{upstream: upstream.taskID, downstream: downstream.taskID}
	if _, exists := d.edgeLabels[key]; !exists {
		upstream.downstreams = append(upstream.downstreams, downstream)
		downstream.upstreams = append(downstream.upstreams, upstream)
	}
	d.edgeLabels[key] = label
}

// cycleLocked returns the task_ids on a cycle of the Dag, closed by the task it starts from
// again, and nil when the Dag is acyclic. The caller holds d.mu. Registration calls it once, so
// building a Dag walks the graph once rather than once per edge.
func (d *DagRef) cycleLocked() []string {
	const (
		unvisited = iota
		onPath
		settled
	)
	state := make(map[*TaskRef]int, len(d.tasks))
	var path []*TaskRef
	// walk is (non-tailrec-eligible) recursive, so a Dag whose dependencies nest deeper than the
	// stack takes would overflow it. Airflow's own Dag serialization recurses over a Dag too.
	var walk func(task *TaskRef) []string
	walk = func(task *TaskRef) []string {
		state[task] = onPath
		path = append(path, task)
		for _, downstream := range task.downstreams {
			switch state[downstream] {
			case onPath:
				// The cycle is the path from downstream onwards, closed by downstream again.
				return append(taskIDs(path[slices.Index(path, downstream):]), downstream.taskID)
			case unvisited:
				if cycle := walk(downstream); cycle != nil {
					return cycle
				}
			}
		}
		path = path[:len(path)-1]
		state[task] = settled
		return nil
	}
	for _, task := range d.tasks {
		if state[task] == unvisited {
			if cycle := walk(task); cycle != nil {
				return cycle
			}
		}
	}
	return nil
}

func taskIDs(tasks []*TaskRef) []string {
	ids := make([]string, len(tasks))
	for i, task := range tasks {
		ids[i] = task.taskID
	}
	return ids
}
