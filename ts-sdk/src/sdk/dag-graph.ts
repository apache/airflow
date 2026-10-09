/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// A Dag's task graph, resolved from the edges its author declared. Both the serializer, which writes
// it into the serialized Dag, and the operators that act on a task's downstream read it.

import {
  getDagOrderEdges,
  getDagTaskGroups,
  getDagTaskInputs,
  isTaskRef,
  type Dag,
  type TaskGroupRecord,
} from "./dag.js";

/**
 * Each task's direct downstream task IDs, with every group endpoint expanded, as
 * `downstream_task_ids` of the serialized Dag records them.
 */
export function getDagDownstreamTaskIds(dag: Dag): ReadonlyMap<string, ReadonlySet<string>> {
  return buildDagGraph(dag).downstreamTaskIds;
}

export interface GroupEdgeSets {
  readonly upstreamGroups: Set<string>;
  readonly downstreamGroups: Set<string>;
  readonly upstreamTasks: Set<string>;
  readonly downstreamTasks: Set<string>;
}

/** Both views of a Dag's edges: the task graph, and what each group records. */
export interface DagGraph {
  /** Each task's downstream task IDs, with every group endpoint expanded. */
  readonly downstreamTaskIds: Map<string, Set<string>>;
  readonly groupEdges: Map<string, GroupEdgeSets>;
}

/**
 * Resolve a Dag's two kinds of edge into the two views a serialized Dag holds.
 *
 * An order-only edge with a group at either end lands in both: the group object
 * records it for the UI, and the task graph records it expanded, because the
 * scheduler only ever reads task-to-task edges. A group expands to its *roots*
 * when it is downstream and its *leaves* when it is upstream — an edge into a
 * group reaches the tasks that start it, and one out of a group leaves from the
 * tasks that finish it — which is what `TaskGroup.set_upstream` does in Python.
 */
export function buildDagGraph(dag: Dag): DagGraph {
  const groups = getDagTaskGroups(dag);
  const downstreamTaskIds = new Map<string, Set<string>>();
  const groupEdges = new Map<string, GroupEdgeSets>();
  const link = (upstream: string, downstream: string): void => {
    // Two arguments fed by the same upstream are one edge, as is an order-only
    // edge redeclaring one the wiring already drew.
    const edges = downstreamTaskIds.get(upstream) ?? new Set<string>();
    edges.add(downstream);
    downstreamTaskIds.set(upstream, edges);
  };

  for (const [taskId, inputs] of getDagTaskInputs(dag)) {
    for (const value of Object.values(inputs)) {
      if (isTaskRef(value)) link(value.taskId, taskId);
    }
  }
  const orderEdges = getDagOrderEdges(dag);
  for (const { upstream, downstream } of orderEdges) {
    if (!groups.has(upstream) && !groups.has(downstream)) link(upstream, downstream);
  }

  // Roots and leaves are read off the task-to-task graph, which holds every
  // edge that can sit inside a group by now: wiring, and any order-only edge
  // between two tasks. Python resolves them at `>>` time and so is equally
  // order-sensitive, which is what keeps the two in step.
  const ends = new GroupEnds(groups, downstreamTaskIds);

  // Which group edges each endpoint has, for stepping over a group that holds
  // no tasks.
  const upstreamsOf = new Map<string, Set<string>>();
  const downstreamsOf = new Map<string, Set<string>>();
  for (const { upstream, downstream } of orderEdges) {
    addTo(downstreamsOf, upstream, downstream);
    addTo(upstreamsOf, downstream, upstream);
  }

  /**
   * The tasks an edge endpoint stands for: the task itself, or a group's leaves
   * when it is upstream and its roots when it is downstream.
   *
   * A group holding no tasks has neither, so the edge steps over it and
   * continues along the group edges beyond — `x >> empty >> y` still runs `y`
   * after `x`, as Python's `find_leaves` walk does.
   *
   * On the upstream side that walk has one more step: a group that still comes
   * up empty stands for the group holding it, so an edge out of an empty
   * nested group reaches the tasks around it. Python gives a group standing
   * downstream no such fallback, and neither does this.
   */
  const tasksAt = (id: string, side: "upstream" | "downstream"): string[] => {
    if (!groups.has(id)) return [id];
    const own = side === "upstream" ? ends.leaves(id) : ends.roots(id);
    return own.length > 0 ? own : tasksBeyond(id, side, new Set());
  };
  const tasksBeyond = (
    id: string,
    side: "upstream" | "downstream",
    seen: Set<string>,
  ): string[] => {
    if (seen.has(id)) return [];
    seen.add(id);
    const next = side === "upstream" ? upstreamsOf.get(id) : downstreamsOf.get(id);
    const beyond = [...(next ?? [])].flatMap((other) => {
      if (!groups.has(other)) return [other];
      const own = side === "upstream" ? ends.leaves(other) : ends.roots(other);
      return own.length > 0 ? own : tasksBeyond(other, side, seen);
    });
    if (beyond.length > 0 || side === "downstream") return beyond;
    const parent = groups.get(id)?.parentGroupId;
    if (parent === undefined) return [];
    const own = ends.leaves(parent);
    return own.length > 0 ? own : tasksBeyond(parent, side, seen);
  };
  const setsFor = (groupId: string): GroupEdgeSets => {
    let sets = groupEdges.get(groupId);
    if (!sets) {
      sets = {
        upstreamGroups: new Set(),
        downstreamGroups: new Set(),
        upstreamTasks: new Set(),
        downstreamTasks: new Set(),
      };
      groupEdges.set(groupId, sets);
    }
    return sets;
  };

  for (const { upstream, downstream } of orderEdges) {
    const upstreamIsGroup = groups.has(upstream);
    const downstreamIsGroup = groups.has(downstream);
    if (!upstreamIsGroup && !downstreamIsGroup) continue;

    const from = tasksAt(upstream, "upstream");
    const to = tasksAt(downstream, "downstream");
    for (const tail of from) {
      for (const head of to) link(tail, head);
    }

    if (downstreamIsGroup) {
      const sets = setsFor(downstream);
      for (const tail of from) sets.upstreamTasks.add(tail);
      if (upstreamIsGroup) sets.upstreamGroups.add(upstream);
    }
    // Only a group whose downstream is a plain task records it as a task; when
    // both ends are groups the pair is recorded as a group edge on this side
    // and as the expanded tasks on the other, which is how Python leaves it.
    if (upstreamIsGroup) {
      const sets = setsFor(upstream);
      if (downstreamIsGroup) sets.downstreamGroups.add(downstream);
      else sets.downstreamTasks.add(downstream);
    }
  }
  return { downstreamTaskIds, groupEdges };
}

function addTo(index: Map<string, Set<string>>, key: string, value: string): void {
  const existing = index.get(key) ?? new Set<string>();
  existing.add(value);
  index.set(key, existing);
}

/** The tasks an edge reaches when it points at a group, cached per group. */
class GroupEnds {
  readonly #groups: ReadonlyMap<string, TaskGroupRecord>;
  readonly #downstream: ReadonlyMap<string, ReadonlySet<string>>;
  readonly #members = new Map<string, Set<string>>();

  constructor(
    groups: ReadonlyMap<string, TaskGroupRecord>,
    downstream: ReadonlyMap<string, ReadonlySet<string>>,
  ) {
    this.#groups = groups;
    this.#downstream = downstream;
  }

  /** Tasks in the group with no upstream inside it: where an edge in arrives. */
  roots(groupId: string): string[] {
    const members = this.#membersOf(groupId);
    const hasInternalUpstream = new Set<string>();
    for (const [upstream, downstream] of this.#downstream) {
      if (!members.has(upstream)) continue;
      for (const task of downstream) {
        if (members.has(task)) hasInternalUpstream.add(task);
      }
    }
    return [...members].filter((task) => !hasInternalUpstream.has(task));
  }

  /** Tasks in the group with no downstream inside it: where an edge out leaves. */
  leaves(groupId: string): string[] {
    const members = this.#membersOf(groupId);
    return [...members].filter(
      (task) => ![...(this.#downstream.get(task) ?? [])].some((other) => members.has(other)),
    );
  }

  /** Every task the group holds, nested groups included. */
  #membersOf(groupId: string): Set<string> {
    const cached = this.#members.get(groupId);
    if (cached) return cached;
    const members = new Set<string>();
    const pending = [groupId];
    for (let i = 0; i < pending.length; i += 1) {
      const group = this.#groups.get(pending[i]!);
      if (!group) continue;
      for (const taskId of group.taskIds) members.add(taskId);
      pending.push(...group.childGroupIds);
    }
    this.#members.set(groupId, members);
    return members;
  }
}
