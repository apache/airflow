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

import { describe, expect, it } from "vitest";
import { findTaskCycle, type TaskEdge } from "../../src/sdk/cycle.js";

/** `"a>b c>d"` as edges, so a graph reads as the shape it is. */
function edges(spec: string): TaskEdge[] {
  if (spec === "") return [];
  return spec.split(" ").map((pair) => {
    const [upstream, downstream] = pair.split(">");
    return { upstream: upstream!, downstream: downstream! };
  });
}

describe("findTaskCycle", () => {
  it.each([
    ["no tasks at all", [], ""],
    ["one task and no edges", ["a"], ""],
    ["a chain", ["a", "b", "c"], "a>b b>c"],
    ["a diamond, where one task is reached twice", ["a", "b", "c", "d"], "a>b a>c b>d c>d"],
    ["a fan-out", ["a", "b", "c"], "a>b a>c"],
    ["a fan-in", ["a", "b", "c"], "a>c b>c"],
    ["two disconnected components", ["a", "b", "c", "d"], "a>b c>d"],
    ["an isolated task beside a chain", ["a", "b", "lonely"], "a>b"],
  ])("finds none in %s", (_label, taskIds, spec) => {
    expect(findTaskCycle(taskIds, edges(spec))).toBeUndefined();
  });

  it.each([
    ["a self-edge", ["a"], "a>a", ["a", "a"]],
    ["a two-task cycle", ["a", "b"], "a>b b>a", ["a", "b", "a"]],
    ["a three-task cycle", ["a", "b", "c"], "a>b b>c c>a", ["a", "b", "c", "a"]],
  ])("reports the tasks on %s", (_label, taskIds, spec, expected) => {
    expect(findTaskCycle(taskIds, edges(spec))).toEqual(expected);
  });

  it("reports only the cycle, not the path that led into it", () => {
    // `entry` reaches the cycle but is not on it, so naming it would send a
    // reader looking for an edge that does not exist.
    expect(findTaskCycle(["entry", "a", "b"], edges("entry>a a>b b>a"))).toEqual(["a", "b", "a"]);
  });

  it("finds a cycle in a component the search reaches second", () => {
    expect(findTaskCycle(["a", "b", "x", "y"], edges("a>b x>y y>x"))).toEqual(["x", "y", "x"]);
  });

  it("ignores an edge naming a task the Dag does not hold", () => {
    // The Dag reports an unknown endpoint in its own terms; it cannot be part
    // of a cycle among the tasks that do exist.
    expect(findTaskCycle(["a"], edges("a>ghost ghost>a"))).toBeUndefined();
  });

  it("handles a chain far longer than the call stack would take", () => {
    const taskIds = Array.from({ length: 50_000 }, (_, i) => `t${i}`);
    const chain = taskIds.slice(0, -1).map((upstream, i) => ({
      upstream,
      downstream: taskIds[i + 1]!,
    }));

    expect(findTaskCycle(taskIds, chain)).toBeUndefined();
    expect(
      findTaskCycle(taskIds, [...chain, { upstream: "t49999", downstream: "t0" }]),
    ).toHaveLength(taskIds.length + 1);
  });

  it("does not re-walk a task the search has already finished with", () => {
    // A wide diamond lattice: exponential if finished tasks were revisited.
    const taskIds = ["start"];
    const wide: TaskEdge[] = [];
    let previous = "start";
    for (let layer = 0; layer < 30; layer += 1) {
      const left = `l${layer}`;
      const right = `r${layer}`;
      const join = `j${layer}`;
      taskIds.push(left, right, join);
      wide.push(
        { upstream: previous, downstream: left },
        { upstream: previous, downstream: right },
        { upstream: left, downstream: join },
        { upstream: right, downstream: join },
      );
      previous = join;
    }

    expect(findTaskCycle(taskIds, wide)).toBeUndefined();
  });
});
