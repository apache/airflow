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
	"unicode/utf8"
)

// groupIDMaxLength is the longest group_id that Python's validate_group_key accepts, counted in
// characters.
const groupIDMaxLength = 200

// The suffixes of the two join nodes that the Airflow UI draws for every task group, as Python's
// TaskGroup.upstream_join_id and TaskGroup.downstream_join_id name them. Both IDs belong to the
// namespace that task_ids and group_ids share, so no task or group can take them.
const (
	upstreamJoinSuffix   = ".upstream_join_id"
	downstreamJoinSuffix = ".downstream_join_id"
)

// TaskGroupRef is a task group that [DagRef.TaskGroup] or [TaskGroupRef.TaskGroup] added to a
// Dag. It offers the methods of the Dag that add tasks and groups, and the group_id prefixes the
// ID of each task and group it adds, unless its [TaskGroupSpec] sets PrefixGroupID to false. A
// TaskGroupRef is a [Node], so [TaskGroupRef.Before] and [TaskGroupRef.After] order the whole
// group against a task or another group.
type TaskGroupRef struct {
	dag *DagRef
	// parent is the group that the group was added through. It is nil for a group that
	// DagRef.TaskGroup added.
	parent  *TaskGroupRef
	groupID string
	spec    TaskGroupSpec
	// children holds the tasks and the groups added through the group, in the order they were
	// added. Each is a *TaskRef or a *TaskGroupRef. The groups of a Dag form a tree, which a
	// serialized Dag carries in task_group.children.
	children []Node
}

// TaskGroup adds a task group to the Dag and returns it. Add the tasks of the group through the
// group, and order the group as a whole against a task or another group:
//
//	group := dag.TaskGroup("transform")
//	cleaned := group.Task(cleanRows)
//	validated := group.Task(validateRows, airflow.Inputs(cleaned))
//
//	extracted.Before(group)  // extract >> transform
//
// The group_id prefixes the ID of each task and group added through the group, as Python's
// prefix_group_id does. So the tasks above are transform.cleanRows and transform.validateRows.
// [TaskGroupRef.TaskGroup] nests a group in another, and the prefixes nest with it. Set
// PrefixGroupID in a [TaskGroupSpec] to false to keep the IDs of the tasks and groups added
// through the group as they are written. Then they have to be unique across the Dag.
//
// An optional TaskGroupSpec holds the rest of the group's attributes, and TaskGroup panics if it
// gets more than one TaskGroupSpec.
//
// Renaming a group renames each task whose task_id the group_id prefixes. Airflow treats a
// renamed task as a new task, as it does when the Go function of a task is renamed.
//
// Task groups and tasks share one namespace of IDs, as they do in Python. The IDs of the two join
// nodes that the Airflow UI draws for a group, the group_id followed by .upstream_join_id or
// .downstream_join_id, are part of it too. Python reserves them only once the group exists, so it
// lets a task added before the group take one. TaskGroup panics in that case too.
//
// TaskGroup panics if:
//   - groupID is empty, is longer than 200 characters, or holds a character other than a letter,
//     a digit, an underscore or a dash, as Python's validate_group_key requires
//   - spec holds more than one TaskGroupSpec
//   - a task, a task group or a join node already takes the ID that the group would get, or the
//     ID of one of its join nodes
//   - the Dag is already registered
func (d *DagRef) TaskGroup(groupID string, spec ...TaskGroupSpec) *TaskGroupRef {
	return d.addGroup("airflow.DagRef.TaskGroup", nil, groupID, spec)
}

// TaskGroup adds a task group inside g and returns it. The new group is
// [DagRef.TaskGroup] one level down: its group_id takes the group_id of g as a prefix, unless the
// TaskGroupSpec of g sets PrefixGroupID to false.
//
//	transform := dag.TaskGroup("transform")
//	checks := transform.TaskGroup("checks")
//	checks.Task(checkNulls)  // the task transform.checks.checkNulls
//
// TaskGroup panics for the reasons that DagRef.TaskGroup lists. It also panics if g is not the
// TaskGroupRef that DagRef.TaskGroup or TaskGroupRef.TaskGroup returned, for example a copy of it.
func (g *TaskGroupRef) TaskGroup(groupID string, spec ...TaskGroupSpec) *TaskGroupRef {
	const method = "airflow.TaskGroupRef.TaskGroup"
	return g.groupDag(method).addGroup(method, g, groupID, spec)
}

// Task adds a task that runs fn to the Dag of g, inside g, and returns the new task. It is
// [DagRef.Task] for a task of the group: fn, the options and the task_id follow the same rules,
// and the group_id of g then prefixes the task_id, unless the TaskGroupSpec of g sets
// PrefixGroupID to false. The group_id also prefixes a TaskID that a TaskSpec sets:
//
//	group := dag.TaskGroup("transform")
//	group.Task(cleanRows)                                    // transform.cleanRows
//	group.Task(validateRows, airflow.TaskSpec{TaskID: "v"})  // transform.v
//
// [Inputs] takes any task of the Dag, inside the group or not.
//
// Task panics for the reasons that DagRef.Task lists, and names the task by the task_id that the
// group gives it. It also panics if g is not the TaskGroupRef that DagRef.TaskGroup or
// TaskGroupRef.TaskGroup returned, for example a copy of it.
func (g *TaskGroupRef) Task(fn any, opts ...TaskOption) *TaskRef {
	const method = "airflow.TaskGroupRef.Task"
	return g.groupDag(method).addTask(method, g, fn, opts, nil)
}

// If adds a condition to the Dag of g, inside g, and returns it. It is [DagRef.If] for a
// condition of the group, and the group_id of g prefixes the task_id of the condition task as
// [TaskGroupRef.Task] describes. [IfRef.Then] and [IfRef.Else] take any task of the Dag, inside
// the group or not.
//
// If panics for the reasons that DagRef.If and TaskGroupRef.Task list.
func (g *TaskGroupRef) If(fn any, opts ...TaskOption) *IfRef {
	const method = "airflow.TaskGroupRef.If"
	return g.groupDag(method).addIf(method, g, fn, opts)
}

// Switch adds a switch to the Dag of g, inside g, and returns it. It is [DagRef.Switch] for a
// switch of the group, and the group_id of g prefixes the task_id of the decider task as
// [TaskGroupRef.Task] describes. [SwitchRef.Case] takes any task of the Dag, inside the group or
// not.
//
// Switch panics for the reasons that DagRef.Switch and TaskGroupRef.Task list.
func (g *TaskGroupRef) Switch(fn any, opts ...TaskOption) *SwitchRef {
	const method = "airflow.TaskGroupRef.Switch"
	return g.groupDag(method).addSwitch(method, g, fn, opts)
}

func (*TaskGroupRef) node() {}

// Before makes the group an upstream of every node, which is Python's
// transform >> [load, report]:
//
//	transform.Before(loaded, reported)
//
// It is [TaskRef.Before] with the group at one end. A node can be a task or another group, and
// Before returns the nodes it was given as one Node, so a chain reads as it does for a task:
// extracted.Before(transform).Before(loaded) is extract >> transform >> load.
//
// An edge from a group stands for an edge from each of its last tasks, and an edge to a group for
// an edge to each of its first tasks. The first tasks of a group are the tasks in it, nested
// groups included, that no edge reaches from another task in the group, and its last tasks are
// those that no edge leaves for another task in the group, as Python's TaskGroup.get_roots and
// TaskGroup.get_leaves find them.
//
// [BundleRef.Register] expands the group edges one at a time, in the order they were first
// declared. Each expansion reads every task, every edge declared between two tasks, and the task
// edges that earlier group edges expanded into. So an author can add the tasks of a group, and
// the edges between them, after putting the group on an edge. The order of two group edges
// matters when one of them is inside a group that the other reaches: an edge between two groups
// inside transform changes the first and last tasks of transform only for the edges to and from
// transform declared after it. Python works out a group edge when >> runs, so the two agree when
// a Dag declares its group edges after its tasks and the edges between them, every group at an end
// of a group edge holds a task, and no [Label] sits on an edge whose receiver is inside a task
// group.
//
// Register steps over a group that holds no task: an edge to the group continues along each edge
// from it, and an edge from the group continues back along each edge to it, whenever those were
// declared. So extracted.Before(empty).Before(loaded) runs load after extract, as Python's
// extract >> empty >> load does. What Python does with an empty group depends on how and when its
// edges are declared: load << empty << extract adds no edge, and an empty group with no task
// before it falls back to the last tasks of the group that holds it, or of the whole Dag.
//
// A [Label] on an edge to or from a group labels that edge, and none of the edges between tasks
// that it stands for. Depending on which end is the receiver and which groups hold the ends,
// Python labels those edges as well, as extract >> Label("rows") >> transform does when no group
// holds extract, or replaces the receiver with a group that holds it.
//
// Before panics for the reasons that TaskRef.Before lists. It also panics if:
//   - g or a node is a *TaskGroupRef that DagRef.TaskGroup or TaskGroupRef.TaskGroup did not
//     return, such as a copy of one
//   - a node is g itself, a task or a group inside g, or a group that holds g
func (g *TaskGroupRef) Before(nodes ...Node) Node { return declareEdges(g, nodes, dirBefore) }

// After makes the group a downstream of every node, which is Python's transform << extract:
//
//	transform.After(extracted)
//
// It is Before with the direction reversed, and it panics for the same reasons.
func (g *TaskGroupRef) After(nodes ...Node) Node { return declareEdges(g, nodes, dirAfter) }

// groupDag returns the Dag of g. It panics if g is nil or zero, which no method of g can serve.
// The Dag checks that g is the TaskGroupRef it returned once it holds its lock.
func (g *TaskGroupRef) groupDag(method string) *DagRef {
	if g == nil || g.dag == nil {
		panic(method + ": DagRef.TaskGroup or TaskGroupRef.TaskGroup did not return " +
			"the *airflow.TaskGroupRef")
	}
	return g.dag
}

// checkGroupLocked panics unless group is nil or a group that d added. A copy of a TaskGroupRef
// has the dag and group_id of the original, so only the identity of the pointer tells them apart.
// The caller holds d.mu.
func (d *DagRef) checkGroupLocked(method string, group *TaskGroupRef) {
	if group != nil && d.groupsByID[group.groupID] != group {
		panic(fmt.Sprintf(
			"%s: Dag %q got a *airflow.TaskGroupRef that DagRef.TaskGroup or "+
				"TaskGroupRef.TaskGroup did not return",
			method, d.dagID,
		))
	}
}

// childID returns the ID that a task or group added through g with the given ID gets. g is nil
// for one added to the Dag itself.
func (g *TaskGroupRef) childID(id string) string {
	if g == nil || (g.spec.PrefixGroupID != nil && !*g.spec.PrefixGroupID) {
		return id
	}
	return g.groupID + "." + id
}

// holds reports whether node is inside g, at any depth. A group does not hold itself.
func (g *TaskGroupRef) holds(node nodeEndpoint) bool {
	for parent := node.container(); parent != nil; parent = parent.parent {
		if parent == g {
			return true
		}
	}
	return false
}

// addGroup adds a task group for DagRef.TaskGroup and TaskGroupRef.TaskGroup. parent is the group
// it is added through, and nil for DagRef.TaskGroup.
func (d *DagRef) addGroup(
	method string, parent *TaskGroupRef, groupID string, spec []TaskGroupSpec,
) *TaskGroupRef {
	if len(spec) > 1 {
		panic(fmt.Sprintf(
			"%s: task group %q of Dag %q got %d airflow.TaskGroupSpec values; "+
				"set all of the group's attributes in one TaskGroupSpec",
			method, parent.childID(groupID), d.dagID, len(spec),
		))
	}
	if err := checkGroupID(groupID); err != nil {
		panic(fmt.Sprintf("%s: Dag %q: %v", method, d.dagID, err))
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.registered {
		panic(fmt.Sprintf(
			"%s: Dag %q has already been registered; add every task group before Register",
			method, d.dagID,
		))
	}
	d.checkGroupLocked(method, parent)
	fullID := parent.childID(groupID)
	for _, id := range []string{fullID, fullID + upstreamJoinSuffix, fullID + downstreamJoinSuffix} {
		if taken := d.describeIDLocked(id); taken != "" {
			panic(fmt.Sprintf(
				"%s: Dag %q cannot add task group %q, because %s already takes the ID %q; "+
					"pass another group_id",
				method, d.dagID, fullID, taken, id,
			))
		}
	}

	group := &TaskGroupRef{dag: d, parent: parent, groupID: fullID}
	if len(spec) == 1 {
		group.spec = copySpec(spec[0])
	}
	if d.groupsByID == nil {
		d.groupsByID = make(map[string]*TaskGroupRef)
	}
	d.groupsByID[fullID] = group
	d.groups = append(d.groups, group)
	if parent != nil {
		parent.children = append(parent.children, group)
	}
	return group
}

// checkGroupID checks a group_id as Python's validate_group_key does. Python matches the ID
// against ^[\w-]+$, and its \w takes the characters for which str.isalnum is true, and the
// underscore. Python's $ also lets a trailing newline through, and checkGroupID does not.
func checkGroupID(groupID string) error {
	if groupID == "" {
		return fmt.Errorf("a task group needs a group_id, and got an empty one")
	}
	if !utf8.ValidString(groupID) {
		return fmt.Errorf("group_id %q is not valid UTF-8", groupID)
	}
	if n := utf8.RuneCountInString(groupID); n > groupIDMaxLength {
		return fmt.Errorf(
			"group_id %q has %d characters; a group_id has at most %d",
			groupID, n, groupIDMaxLength,
		)
	}
	for _, r := range groupID {
		if isWordRune(r) || r == '-' {
			continue
		}
		var hint string
		if r == '.' {
			hint = "; nest a group in another with TaskGroupRef.TaskGroup"
		}
		return fmt.Errorf(
			"group_id %q holds %q; a group_id holds only letters, digits, underscores and dashes%s",
			groupID, r, hint,
		)
	}
	return nil
}

// describeIDLocked names what takes id in the namespace that the tasks and task groups of d
// share, and returns the empty string when id is free. The caller holds d.mu.
func (d *DagRef) describeIDLocked(id string) string {
	if _, ok := d.tasksByID[id]; ok {
		return fmt.Sprintf("task %q", id)
	}
	if _, ok := d.groupsByID[id]; ok {
		return fmt.Sprintf("task group %q", id)
	}
	for _, suffix := range []string{upstreamJoinSuffix, downstreamJoinSuffix} {
		if groupID, ok := strings.CutSuffix(id, suffix); ok && d.groupsByID[groupID] != nil {
			return fmt.Sprintf("a join node of task group %q", groupID)
		}
	}
	return ""
}

// groupEdge is an edge that has a task group at one end or both. It stands for the edges between
// the tasks of its ends, which [DagRef.expandGroupEdgesLocked] works out at registration.
type groupEdge struct {
	upstream, downstream nodeEndpoint
}

// addGroupEdgeLocked records one edge of d that has a task group at one end or both. The caller
// holds d.mu, and settles the label of an edge that d already has with [mergeLabel] first.
func (d *DagRef) addGroupEdgeLocked(upstream, downstream nodeEndpoint, label string) {
	if d.groupEdgeLabels == nil {
		d.groupEdgeLabels = make(map[edgeKey]string)
	}
	key := edgeKey{upstream: upstream.id(), downstream: downstream.id()}
	if _, exists := d.groupEdgeLabels[key]; !exists {
		d.groupEdges = append(d.groupEdges, groupEdge{
			upstream:   nodeEndpoint{task: upstream.task, group: upstream.group},
			downstream: nodeEndpoint{task: downstream.task, group: downstream.group},
		})
	}
	d.groupEdgeLabels[key] = label
}

// expandGroupEdgesLocked records the edges between tasks that the group edges of d stand for, and
// returns the keys of the edges it added. The caller holds d.mu.
//
// It expands the group edges in the order they were first declared, each from the Dag as it stands
// by then: every task, every edge between two tasks, and the edges that the group edges before it
// added. Stepping over a group with no task follows every group edge of that group, whenever it
// was declared. The TypeScript SDK expands the group edges of a Dag it serializes by the same
// rule, so that a Dag built alike in both SDKs ends up with the same task edges.
func (d *DagRef) expandGroupEdgesLocked() []expandedEdge {
	expansion := newGroupExpansion(d.groupEdges)
	var added []expandedEdge
	for _, edge := range d.groupEdges {
		from := edgeKey{upstream: edge.upstream.id(), downstream: edge.downstream.id()}
		upstreams := expansion.tasksAt(edge.upstream, false)
		downstreams := expansion.tasksAt(edge.downstream, true)
		for _, upstream := range upstreams {
			for _, downstream := range downstreams {
				key := edgeKey{upstream: upstream.taskID, downstream: downstream.taskID}
				if _, exists := d.edgeLabels[key]; exists {
					continue
				}
				// The label of a group edge stays on the group edge, as TaskGroupRef.Before says.
				// Stepping over a group that holds no task can lead back to the task it started
				// from, as extract >> empty >> extract does, and the cycle check reports that.
				d.addEdgeLocked(upstream, downstream, "")
				added = append(added, expandedEdge{key: key, from: from})
			}
		}
	}
	return added
}

// expandedEdge is an edge between tasks that expandGroupEdgesLocked added, with the group edge
// that it stands for.
type expandedEdge struct {
	key, from edgeKey
}

// groupEdgesOn names the group edges that the expanded edges on cycle stand for, in the order the
// cycle runs through them. cycle is a list of task_ids that ends with the one it starts from.
func groupEdgesOn(cycle []string, expanded []expandedEdge) []string {
	from := make(map[edgeKey]edgeKey, len(expanded))
	for _, edge := range expanded {
		from[edge.key] = edge.from
	}
	var names []string
	named := make(map[edgeKey]bool)
	for i := 0; i+1 < len(cycle); i++ {
		group, ok := from[edgeKey{upstream: cycle[i], downstream: cycle[i+1]}]
		if ok && !named[group] {
			named[group] = true
			names = append(names, group.upstream+" -> "+group.downstream)
		}
	}
	return names
}

// removeEdgesLocked takes the edges that expandGroupEdgesLocked added back out of d. Registration
// calls it when it rejects the Dag, so that the Dag holds only the edges its author declared. The
// caller holds d.mu.
func (d *DagRef) removeEdgesLocked(expanded []expandedEdge) {
	for _, edge := range expanded {
		key := edge.key
		upstream, downstream := d.tasksByID[key.upstream], d.tasksByID[key.downstream]
		upstream.downstreams = slices.DeleteFunc(upstream.downstreams, func(task *TaskRef) bool {
			return task == downstream
		})
		downstream.upstreams = slices.DeleteFunc(downstream.upstreams, func(task *TaskRef) bool {
			return task == upstream
		})
		delete(d.edgeLabels, key)
	}
}

// groupExpansion holds what expandGroupEdgesLocked needs besides the edges between tasks: the
// tasks in each group, which no expansion changes, and the group edges of each group.
type groupExpansion struct {
	members map[*TaskGroupRef][]*TaskRef
	inside  map[*TaskGroupRef]map[*TaskRef]bool
	// upstreams holds, for each group, the nodes whose group edges reach it, and downstreams the
	// nodes that its group edges reach, in the order the edges were first declared.
	upstreams, downstreams map[*TaskGroupRef][]nodeEndpoint
}

func newGroupExpansion(edges []groupEdge) *groupExpansion {
	expansion := &groupExpansion{
		members:     make(map[*TaskGroupRef][]*TaskRef),
		inside:      make(map[*TaskGroupRef]map[*TaskRef]bool),
		upstreams:   make(map[*TaskGroupRef][]nodeEndpoint),
		downstreams: make(map[*TaskGroupRef][]nodeEndpoint),
	}
	for _, edge := range edges {
		if group := edge.downstream.group; group != nil {
			expansion.upstreams[group] = append(expansion.upstreams[group], edge.upstream)
		}
		if group := edge.upstream.group; group != nil {
			expansion.downstreams[group] = append(expansion.downstreams[group], edge.downstream)
		}
	}
	return expansion
}

// tasksAt returns the tasks that an end of a group edge stands for. A task stands for itself, and
// a group for its first tasks when first is true, as the downstream end of an edge, and for its
// last tasks otherwise. A group that has none stands for the tasks beyond it.
func (e *groupExpansion) tasksAt(end nodeEndpoint, first bool) []*TaskRef {
	if end.group == nil {
		return []*TaskRef{end.task}
	}
	if found := e.ends(end.group, first); len(found) > 0 {
		return found
	}
	var found []*TaskRef
	e.tasksBeyond(end.group, first, make(map[*TaskGroupRef]bool), &found)
	return found
}

// tasksBeyond appends to found the tasks that an edge reaches through group when group has no
// first or last task to stop at. An edge into group continues along the group edges from group
// when first is true, and an edge out of group continues back along the group edges into it
// otherwise, as far as a task or a group with tasks to stop at. The TypeScript SDK steps over
// such a group the same way, and Python's extract >> empty >> load also runs load after extract.
//
// A group has no first task when it holds no task, or when each of its tasks has an upstream task
// inside it, which only a cycle inside the group allows, and likewise for last tasks with
// downstream tasks. Registration rejects such a cycle anyway. seen holds the groups already
// stepped over, so that a loop of group edges between such groups ends.
func (e *groupExpansion) tasksBeyond(
	group *TaskGroupRef, first bool, seen map[*TaskGroupRef]bool, found *[]*TaskRef,
) {
	if seen[group] {
		return
	}
	seen[group] = true
	next := e.upstreams[group]
	if first {
		next = e.downstreams[group]
	}
	for _, node := range next {
		if node.group == nil {
			*found = append(*found, node.task)
			continue
		}
		if ends := e.ends(node.group, first); len(ends) > 0 {
			*found = append(*found, ends...)
			continue
		}
		e.tasksBeyond(node.group, first, seen, found)
	}
}

// ends returns the first tasks of group when first is true, and its last tasks otherwise, as the
// Dag stands: the tasks in the group that no edge reaches from another task in it, or that no edge
// leaves for another task in it.
func (e *groupExpansion) ends(group *TaskGroupRef, first bool) []*TaskRef {
	members, inside := e.membersOf(group)
	isInside := func(task *TaskRef) bool { return inside[task] }
	var found []*TaskRef
	for _, task := range members {
		neighbours := task.downstreams
		if first {
			neighbours = task.upstreams
		}
		if !slices.ContainsFunc(neighbours, isInside) {
			found = append(found, task)
		}
	}
	return found
}

// membersOf returns the tasks in group, nested groups included, as a slice and as a set. It lists
// the tasks of a group before those of the groups nested in it, level by level.
func (e *groupExpansion) membersOf(group *TaskGroupRef) ([]*TaskRef, map[*TaskRef]bool) {
	if inside, ok := e.inside[group]; ok {
		return e.members[group], inside
	}
	var members []*TaskRef
	pending := []*TaskGroupRef{group}
	for i := 0; i < len(pending); i++ {
		for _, child := range pending[i].children {
			if task, ok := child.(*TaskRef); ok {
				members = append(members, task)
			}
		}
		for _, child := range pending[i].children {
			if nested, ok := child.(*TaskGroupRef); ok {
				pending = append(pending, nested)
			}
		}
	}
	inside := make(map[*TaskRef]bool, len(members))
	for _, task := range members {
		inside[task] = true
	}
	e.members[group], e.inside[group] = members, inside
	return members, inside
}
