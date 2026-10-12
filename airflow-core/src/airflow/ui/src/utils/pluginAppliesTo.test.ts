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

import type {
  DAGResponse,
  DAGRunResponse,
  ExternalViewResponse,
  PluginAppliesToResponse,
  TaskInstanceResponse,
  TaskResponse,
} from "openapi/requests/types.gen";

import {
  type AppliesToContext,
  hasAppliesToCriteria,
  isAppliesToPending,
  matchesAppliesTo,
} from "./pluginAppliesTo";

// These fixtures carry only the fields the tests address, so they are cast through `unknown`
// rather than spelling out every field of the full response types.
const makeDag = (dagId: string, tagNames: Array<string>): DAGResponse =>
  ({
    dag_id: dagId,
    is_paused: false,
    tags: tagNames.map((name) => ({ dag_display_name: dagId, dag_id: dagId, name })),
  }) as unknown as DAGResponse;

const makeDagRun = (state: string | null): DAGRunResponse =>
  ({ dag_id: "etl_sales", dag_run_id: "manual__1", state }) as unknown as DAGRunResponse;

const makeTask = (taskId: string, className: string): TaskResponse =>
  ({
    class_ref: { class_name: className, module_path: "some.module" },
    operator_name: className,
    task_id: taskId,
  }) as unknown as TaskResponse;

const makeTaskInstance = (overrides: Record<string, unknown> = {}): TaskInstanceResponse =>
  ({
    dag_id: "etl_sales",
    map_index: -1,
    operator: "KubernetesPodOperator",
    operator_name: "KubernetesPodOperator",
    state: "failed",
    task_id: "train_model",
    try_number: 2,
    ...overrides,
  }) as unknown as TaskInstanceResponse;

const makeView = (
  appliesTo?: PluginAppliesToResponse,
  destination: ExternalViewResponse["destination"] = "dag_run",
): ExternalViewResponse => ({
  applies_to: appliesTo,
  destination,
  href: "/plugin/example",
  name: "Example",
  url_route: "example",
});

const dag = makeDag("etl_sales", ["ml", "prod"]);

const dagContext: AppliesToContext = { dag, isLoading: false };
const dagRunContext: AppliesToContext = { dag, dagRun: makeDagRun("failed"), isLoading: false };
const taskContext: AppliesToContext = {
  dag,
  isLoading: false,
  task: makeTask("train_model", "KubernetesPodOperator"),
};
const taskInstanceContext: AppliesToContext = {
  dag,
  dagRun: makeDagRun("failed"),
  isLoading: false,
  taskInstance: makeTaskInstance(),
};

describe("matchesAppliesTo — unqualified paths", () => {
  it("shows a contribution with no applies_to everywhere", () => {
    expect(matchesAppliesTo(makeView(), dagRunContext)).toBe(true);
    expect(matchesAppliesTo(makeView({}), dagRunContext)).toBe(true);
  });

  // The case that motivated replacing the closed criteria set.
  it("matches a Dag Run's own state", () => {
    expect(matchesAppliesTo(makeView({ state: ["failed"] }), dagRunContext)).toBe(true);
    expect(matchesAppliesTo(makeView({ state: ["success"] }), dagRunContext)).toBe(false);
  });

  it("roots an unqualified path at the entity the destination is about", () => {
    const view = makeView({ state: ["failed"] }, "task_instance");

    // Same path, different root: the task instance's state, not the run's.
    expect(matchesAppliesTo(view, taskInstanceContext)).toBe(true);
    expect(
      matchesAppliesTo(view, {
        ...taskInstanceContext,
        taskInstance: makeTaskInstance({ state: "success" }),
      }),
    ).toBe(false);
  });

  it("matches any of the listed values", () => {
    const view = makeView({ state: ["failed", "upstream_failed"] });

    expect(matchesAppliesTo(view, dagRunContext)).toBe(true);
    expect(matchesAppliesTo(view, { ...dagRunContext, dagRun: makeDagRun("queued") })).toBe(false);
  });

  it("compares non-string fields as strings", () => {
    expect(matchesAppliesTo(makeView({ try_number: ["2"] }, "task_instance"), taskInstanceContext)).toBe(
      true,
    );
    expect(matchesAppliesTo(makeView({ map_index: ["-1"] }, "task_instance"), taskInstanceContext)).toBe(
      true,
    );
    expect(matchesAppliesTo(makeView({ is_paused: ["false"] }, "dag"), dagContext)).toBe(true);
  });
});

describe("matchesAppliesTo — qualified paths", () => {
  it("reaches a related record by naming it", () => {
    expect(matchesAppliesTo(makeView({ "dag.dag_id": ["etl_sales"] }), dagRunContext)).toBe(true);
    expect(matchesAppliesTo(makeView({ "dag.dag_id": ["other"] }), dagRunContext)).toBe(false);
  });

  it("fans out across an array, matching if any element does", () => {
    expect(matchesAppliesTo(makeView({ "dag.tags.name": ["ml"] }), dagRunContext)).toBe(true);
    expect(matchesAppliesTo(makeView({ "dag.tags.name": ["finance"] }), dagRunContext)).toBe(false);
  });

  it("treats an empty array as no match, not as unevaluable", () => {
    // A Dag with no tags has definitively answered the question; skipping here would show the
    // contribution on every untagged Dag.
    expect(
      matchesAppliesTo(makeView({ "dag.tags.name": ["ml"] }), {
        dag: makeDag("etl_sales", []),
        isLoading: false,
      }),
    ).toBe(false);
  });

  it("matches an operator class name through the task's class_ref", () => {
    expect(
      matchesAppliesTo(
        makeView({ "task.class_ref.class_name": ["KubernetesPodOperator"] }, "task"),
        taskContext,
      ),
    ).toBe(true);
  });
});

describe("matchesAppliesTo — the three verdicts", () => {
  it("skips a path whose root record the surface does not have", () => {
    // A task-instance path on a Dag page: unevaluable, so it must not fail the match.
    expect(matchesAppliesTo(makeView({ "task_instance.state": ["failed"] }, "dag"), dagContext)).toBe(true);
  });

  it("does not match when the field resolves to null", () => {
    expect(
      matchesAppliesTo(makeView({ state: ["failed"] }), {
        ...dagRunContext,
        dagRun: makeDagRun(null),
      }),
    ).toBe(false);
  });

  it("skips a path the record has no such field for", () => {
    // Indistinguishable at runtime from an author typo, so it has to be skipped: `operator`
    // exists on a task instance but not on a task. Typos are caught at plugin load instead.
    expect(matchesAppliesTo(makeView({ operator: ["KubernetesPodOperator"] }, "task"), taskContext)).toBe(
      true,
    );
  });

  it("skips a segment naming an inherited member rather than an own field", () => {
    // Only own fields count. `in` would have found `toString` on the record's prototype and
    // resolved the path to a function -- which has no comparable form, so the view would have
    // been hidden on the strength of a field the record does not actually have.
    expect(matchesAppliesTo(makeView({ toString: ["[object Object]"] }), dagRunContext)).toBe(true);
  });

  it("does not match when a path stops on an object instead of a leaf", () => {
    // `dag.tags` is a list of objects, so there is nothing to compare. Unlike a bad segment this
    // narrows rather than widens: the path is answerable, and the answer is no.
    expect(matchesAppliesTo(makeView({ "dag.tags": ["ml"] }), dagRunContext)).toBe(false);
  });

  it("skips everything on a destination with no entity record", () => {
    expect(matchesAppliesTo(makeView({ state: ["failed"] }, "nav"), dagRunContext)).toBe(true);
  });
});

describe("matchesAppliesTo — combining paths", () => {
  it("ANDs across paths the surface can evaluate", () => {
    expect(matchesAppliesTo(makeView({ "dag.tags.name": ["ml"], state: ["failed"] }), dagRunContext)).toBe(
      true,
    );
    expect(
      matchesAppliesTo(makeView({ "dag.tags.name": ["finance"], state: ["failed"] }), dagRunContext),
    ).toBe(false);
  });

  // The reason the skip rule exists: an author names both operator sources and the block works
  // on a task page and a task-instance page alike.
  it("lets one block serve a task and a task instance", () => {
    const view = makeView(
      {
        operator: ["KubernetesPodOperator"],
        "task.class_ref.class_name": ["KubernetesPodOperator"],
      },
      "task",
    );

    expect(matchesAppliesTo(view, taskContext)).toBe(true);
    expect(matchesAppliesTo({ ...view, destination: "task_instance" }, taskInstanceContext)).toBe(true);
  });

  // `operator_name` is spelled the same on a task and a task instance, so it needs no second
  // path -- the portable way to target an operator, and what the docs lead with.
  it("targets an operator on either destination with one portable path", () => {
    const view = makeView({ operator_name: ["KubernetesPodOperator"] }, "task");

    expect(matchesAppliesTo(view, taskContext)).toBe(true);
    expect(matchesAppliesTo({ ...view, destination: "task_instance" }, taskInstanceContext)).toBe(true);
  });

  // A task instance page resolves the task record too. `usePluginAppliesToContext` fetches it at
  // the version the instance ran, so the two agree; this pins the matcher's half of that, that
  // each unqualified path reads its own record.
  it("reads the page's own operator when the records disagree", () => {
    const staleContext: AppliesToContext = {
      dag,
      dagRun: makeDagRun("failed"),
      isLoading: false,
      task: makeTask("train_model", "PythonOperator"),
      taskInstance: makeTaskInstance(),
    };

    // Unqualified: the instance's own `operator` decides, and `class_ref.class_name` is absent
    // on a task instance and so is skipped.
    expect(
      matchesAppliesTo(
        makeView(
          {
            "class_ref.class_name": ["KubernetesPodOperator"],
            operator: ["KubernetesPodOperator"],
          },
          "task_instance",
        ),
        staleContext,
      ),
    ).toBe(true);

    // Qualified, which the docs warn against: both records resolve, so the task's current
    // (changed) class is AND-ed in and hides the view on this historical instance.
    expect(
      matchesAppliesTo(
        makeView(
          {
            "task.class_ref.class_name": ["KubernetesPodOperator"],
            "task_instance.operator": ["KubernetesPodOperator"],
          },
          "task_instance",
        ),
        staleContext,
      ),
    ).toBe(false);
  });

  it("ignores a path configured with no values", () => {
    expect(matchesAppliesTo(makeView({ state: [] }), dagRunContext)).toBe(true);
  });
});

describe("isAppliesToPending", () => {
  it("never withholds a contribution without scoping", () => {
    expect(isAppliesToPending(makeView(), { isLoading: true })).toBe(false);
    expect(isAppliesToPending(makeView({}), { isLoading: true })).toBe(false);
  });

  it("withholds a scoped contribution while its context is loading", () => {
    expect(isAppliesToPending(makeView({ state: ["failed"] }), { isLoading: true })).toBe(true);
  });

  it("releases a scoped contribution once its context has resolved", () => {
    expect(isAppliesToPending(makeView({ state: ["failed"] }), dagRunContext)).toBe(false);
  });
});

describe("hasAppliesToCriteria", () => {
  it.each([
    ["no applies_to", undefined, false],
    ["an empty applies_to", {}, false],
    ["only empty value lists", { "dag.dag_id": [], state: [] }, false],
    ["a populated path", { state: ["failed"] }, true],
  ])("reports %s as %s", (_label, appliesTo, expected) => {
    expect(hasAppliesToCriteria(makeView(appliesTo))).toBe(expected);
  });
});
