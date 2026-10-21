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
	// appendEndpoints can read what one stands for.
	node()
}

// nodeEndpoint is one task or one task group that a Node stands for, with the label that
// [LabelRef.On] carries into the edge verb the Node is passed to. Exactly one of task and group is
// set.
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

// nodeSet is the Node that Before and After return, and it holds the nodes that were passed to
// the verb. It carries no labels, because a label belongs to the edges of the verb call that it
// was passed to.
type nodeSet []nodeEndpoint

func (nodeSet) node() {}

func (s nodeSet) Before(nodes ...Node) Node { return declareEdges(s, nodes, dirBefore) }

func (s nodeSet) After(nodes ...Node) Node { return declareEdges(s, nodes, dirAfter) }

// labelledSet is the Node that [LabelRef.On] returns. Every endpoint in a labelledSet carries the
// label of the LabelRef. labelledSet is a separate type so that an edge verb can use a type
// assertion to recognize the Node that On returns, without looking at each endpoint. An empty
// labelledSet carries no label.
type labelledSet []nodeEndpoint

func (labelledSet) node() {}

func (s labelledSet) Before(nodes ...Node) Node { return declareEdges(s, nodes, dirBefore) }

func (s labelledSet) After(nodes ...Node) Node { return declareEdges(s, nodes, dirAfter) }

func (*TaskRef) node() {}

// position names a Node in a panic message, such as airflow.Node.Before: nodes[1]. String builds
// the name only when a panic needs it, so a call that does not panic builds no string.
type position struct {
	where string // the function, such as airflow.Node.Before
	param string // the parameter, such as nodes, or the receiver
	index int    // the index into a variadic parameter, or -1 for a parameter that holds one Node
}

func (p position) String() string {
	if p.index < 0 {
		return p.where + ": " + p.param
	}
	return fmt.Sprintf("%s: %s[%d]", p.where, p.param, p.index)
}

// appendEndpoints appends to set the tasks and task groups that node stands for, with the labels
// that node carries. It returns the extended set. The Node method is unexported, so the types of
// this package are the only Nodes. Only a struct that embeds one of those types reaches the
// default case. at names node in a panic message.
func appendEndpoints(set []nodeEndpoint, node Node, at position) []nodeEndpoint {
	switch node := node.(type) {
	case nil:
		panic(at.String() + " is a nil airflow.Node")
	case *TaskRef:
		if node == nil {
			panic(at.String() + " is a nil *airflow.TaskRef")
		}
		return append(set, nodeEndpoint{task: node})
	case *TaskGroupRef:
		if node == nil {
			panic(at.String() + " is a nil *airflow.TaskGroupRef")
		}
		return append(set, nodeEndpoint{group: node})
	case nodeSet:
		return append(set, node...)
	case labelledSet:
		return append(set, node...)
	default:
		panic(fmt.Sprintf(
			"%s has type %T, which is not a node that package airflow defines", at.String(), node,
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
// To label the edge that reaches a node, pass Label(text).On(node) in place of the node.
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

// LabelRef is an edge label made by [Label]. [LabelRef.On] returns a Node. To label edges, pass
// that Node to Before or After.
type LabelRef struct {
	text string
}

// Label returns a label with the given text. The Python code
// loaded >> Label("when empty") >> notify_empty becomes:
//
//	loaded.Before(Label("when empty").On(emptyNotice))
//
// The text comes before the node passed to On, so it sits between the two tasks in the code. The
// Airflow UI draws the label between the same two tasks in the graph.
//
// Label panics if text is empty or holds only white space.
func Label(text string) *LabelRef {
	if strings.TrimSpace(text) == "" {
		panic("airflow.Label: got a blank label; pass the text to put on the edge")
	}
	return &LabelRef{text: text}
}

// On returns node, and each node in more, as one Node. Pass that Node to an edge verb, and the verb
// puts the label on each edge between its receiver and one of those nodes. With After, those edges
// point at the receiver:
//
//	cleaned.After(Label("always").On(loaded))  // loaded >> Label("always") >> cleanup
//
// Each Node passed to Before or After can carry its own label, so the edges that one call declares
// can have different labels:
//
//	checked.Before(Label("rows found").On(processed), Label("no rows").On(emptyNotice))
//
// When On is given more than one node, the edge to or from each of those nodes gets the same
// label. Python's loaded >> Label("forks") >> [notify_empty, notify_suspicious] becomes:
//
//	loaded.Before(Label("forks").On(emptyNotice, suspiciousNotice))
//
// A label is the Node's only in the verb it is passed to. The Node that On returns stands for the
// nodes themselves. The Node that a verb returns carries no label into the next verb, either as
// its receiver or as an argument. When labels nest, the outer label is used:
// loaded.Before(Label("outer").On(Label("inner").On(cleaned))) labels the edge "outer".
//
// Before and After panic when a label on a task or task group would get no edge:
//   - Their receiver carries a label, so pass the Node that On returns only as an argument.
//     Python's Label("x") >> b drops such a label without an error instead.
//   - They are given a label, but their receiver, such as the Node that loaded.Before() returns,
//     stands for no task or task group. So they declare no edge. A chain that passes an empty
//     slice of nodes to one verb reaches this case when the next verb in the chain is given a
//     label.
//
// When On is given only empty sets, the Node it returns holds no task or task group, so its
// label is on nothing. Before and After then drop the label without an error.
//
// A label declared on an edge that already carries one replaces it, as Python's DAG.set_edge_info
// does.
//
// An [Inputs] edge is labelled by declaring it again, which is idempotent:
//
//	transformed := dag.Task(transform, Inputs(extracted))
//	extracted.Before(Label("rows").On(transformed))
//
// A label on an edge to or from a task group labels that group edge. On an edge from a task that
// no group holds to a group with tasks, it also labels the edges to the first tasks of the group,
// as [TaskGroupRef.Before] describes. If one of those task edges was also declared on its own, or
// was added by an earlier group edge, that task edge keeps the label it already has, or stays
// unlabelled. A label on an edge between two tasks stays on that edge, even when the tasks are in
// different task groups. In that case Python can replace the receiver of the verb with a group
// that holds it, which makes the edge connect other tasks.
//
// On panics if:
//   - l is nil or points to a zero LabelRef
//   - node or a node in more is nil, is a nil *TaskRef or *TaskGroupRef, or is a struct that
//     embeds a Node
func (l *LabelRef) On(node Node, more ...Node) Node {
	const where = "airflow.LabelRef.On"
	if l == nil || l.text == "" {
		panic(where + ": airflow.Label did not return the *airflow.LabelRef")
	}
	labelled := make([]nodeEndpoint, 0, 1+len(more))
	labelled = appendEndpoints(labelled, node, position{where, "node", -1})
	for i, next := range more {
		labelled = appendEndpoints(labelled, next, position{where, "more", i})
	}
	// When a Node given to this On came from another On, the label of l replaces the label of that
	// Node.
	for i := range labelled {
		labelled[i].label = l.text
	}
	return labelledSet(labelled)
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

// verbNames holds the name that panic messages use for each edge verb.
var verbNames = [...]string{dirBefore: "airflow.Node.Before", dirAfter: "airflow.Node.After"}

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
	where := verbNames[dir]
	args := make([]nodeEndpoint, 0, len(nodes))
	for i, node := range nodes {
		args = appendEndpoints(args, node, position{where, "nodes", i})
	}
	ends := appendEndpoints(nil, receiver, position{where, "the receiver", -1})
	all := slices.Concat(ends, args)
	if len(all) == 0 {
		return nodeSet(nil)
	}

	dag := edgeDag(where, all)
	checkReceiverUnlabelled(where, dag, dir, receiver)

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
	// edge. So does a verb called on the empty set that such a verb returned. A label given to this
	// second verb would have no edge to go on.
	if len(ends) == 0 || len(args) == 0 {
		if labelled := describeLabelled(args); labelled != "" {
			panic(fmt.Sprintf(
				"%s: Dag %q: the receiver stands for no task or task group, so there is no edge "+
					"for %s",
				where, dag.dagID, labelled,
			))
		}
		return nodeSet(args)
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
	// The Node that a verb returns carries no label into the next verb. args belongs to this call, so
	// this loop clears the labels of args in place.
	for i := range args {
		args[i].label = ""
	}
	return nodeSet(args)
}

// checkReceiverUnlabelled panics if the receiver of an edge verb carries a label. Only the Node
// that [LabelRef.On] returns carries labels. A label goes on the edges between the receiver and
// the nodes that the verb is given, so a label on the receiver has no edge to go on.
func checkReceiverUnlabelled(where string, dag *DagRef, dir edgeDir, receiver Node) {
	if labelled, ok := receiver.(labelledSet); ok && len(labelled) > 0 {
		panic(fmt.Sprintf(
			"%s: Dag %q: cannot call %s on %s; "+
				"pass airflow.Label(...).On(...) to Before or After as an argument instead",
			where, dag.dagID, dir, describeLabelled(labelled),
		))
	}
}

// describeLabelled names each endpoint in set that carries a label, as in task "load" labelled
// "always". It returns "" when no endpoint carries one. The result goes into a panic message.
func describeLabelled(set []nodeEndpoint) string {
	var labelled []string
	for _, end := range set {
		if end.label != "" {
			labelled = append(labelled, fmt.Sprintf("%s labelled %q", end.describe(), end.label))
		}
	}
	return strings.Join(labelled, ", ")
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
