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

// Finds a cycle in a Dag's task graph.
//
// Only the edges `before`/`after` draw can form one. A cycle through *wiring*
// is unrepresentable rather than rejected: a `TaskRef` exists only once its
// producing call has returned, so there is no way to write one. Both kinds of
// edge are searched together all the same, because a cycle can run through one
// of each — `transform({ extracted })` and then `extract.after(transform)`.

/** A directed edge between two task IDs. */
export interface TaskEdge {
  readonly upstream: string;
  readonly downstream: string;
}

/**
 * The tasks on a cycle, or undefined when there is none.
 *
 * The returned path starts and ends on the same task, so it reads as the cycle
 * it describes: `["a", "b", "a"]` is `a >> b >> a`.
 *
 * Iterative rather than recursive: a Dag of a few thousand chained tasks is
 * within reach for a generated Dag file, and would overflow the stack.
 */
export function findTaskCycle(
  taskIds: Iterable<string>,
  edges: Iterable<TaskEdge>,
): string[] | undefined {
  const downstreamOf = new Map<string, string[]>();
  for (const taskId of taskIds) downstreamOf.set(taskId, []);
  for (const { upstream, downstream } of edges) {
    // An edge naming a task the Dag does not hold cannot be part of a cycle
    // among the tasks it does, and the caller reports it in its own terms.
    downstreamOf.get(upstream)?.push(downstream);
  }

  // Three states, as a depth-first cycle search needs: unvisited, on the
  // current path, and finished. Finding a task already on the path is the
  // cycle; finding a finished one only means the search has been there before.
  const onPath = new Set<string>();
  const finished = new Set<string>();
  const path: string[] = [];

  for (const root of downstreamOf.keys()) {
    if (finished.has(root)) continue;
    // Each frame is a task and how far its downstream list has been walked.
    const stack: { taskId: string; next: number }[] = [{ taskId: root, next: 0 }];
    onPath.add(root);
    path.push(root);

    while (stack.length > 0) {
      const frame = stack[stack.length - 1]!;
      const downstream = downstreamOf.get(frame.taskId) ?? [];
      if (frame.next === downstream.length) {
        finished.add(frame.taskId);
        onPath.delete(frame.taskId);
        path.pop();
        stack.pop();
        continue;
      }
      const next = downstream[frame.next]!;
      frame.next += 1;
      if (onPath.has(next)) return [...path.slice(path.indexOf(next)), next];
      if (finished.has(next) || !downstreamOf.has(next)) continue;
      onPath.add(next);
      path.push(next);
      stack.push({ taskId: next, next: 0 });
    }
  }
  return undefined;
}
