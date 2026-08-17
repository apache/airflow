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

// These fixtures carry only the fields the matcher reads, so they are cast through
// `unknown` rather than spelling out every field of the full response types.
const makeDag = (dagId: string, tagNames: Array<string>): DAGResponse =>
  ({
    dag_id: dagId,
    tags: tagNames.map((name) => ({ dag_display_name: dagId, dag_id: dagId, name })),
  }) as unknown as DAGResponse;

const makeTask = (taskId: string, className: string, operatorName?: string): TaskResponse =>
  ({
    class_ref: { class_name: className, module_path: "some.module" },
    operator_name: operatorName ?? className,
    task_id: taskId,
  }) as unknown as TaskResponse;

const makeTaskInstance = (taskId: string, operator: string, operatorName?: string): TaskInstanceResponse =>
  ({
    operator,
    operator_name: operatorName ?? operator,
    task_id: taskId,
  }) as unknown as TaskInstanceResponse;

const makeView = (appliesTo?: PluginAppliesToResponse): ExternalViewResponse => ({
  applies_to: appliesTo,
  destination: "dag_run",
  href: "/plugin/example",
  name: "Example",
  url_route: "example",
});

const dag = makeDag("etl_sales", ["ml", "prod"]);

// A Dag-level page: no task or task instance in scope.
const dagContext: AppliesToContext = { dag, isLoading: false };

const taskContext: AppliesToContext = {
  dag,
  isLoading: false,
  task: makeTask("train_model", "KubernetesPodOperator"),
};

const taskInstanceContext: AppliesToContext = {
  dag,
  isLoading: false,
  taskInstance: makeTaskInstance("train_model", "KubernetesPodOperator"),
};

describe("matchesAppliesTo", () => {
  it("shows a view with no applies_to everywhere", () => {
    expect(matchesAppliesTo(makeView(), dagContext)).toBe(true);
    expect(matchesAppliesTo(makeView(), { isLoading: false })).toBe(true);
  });

  it("shows a view whose applies_to has no criteria", () => {
    expect(matchesAppliesTo(makeView({}), dagContext)).toBe(true);
  });

  it("treats an empty criteria list as unset", () => {
    expect(matchesAppliesTo(makeView({ dag_ids: [] }), dagContext)).toBe(true);
  });

  it.each([
    ["a matching tag", { dag_tags: ["ml"] }, true],
    ["a non-matching tag", { dag_tags: ["finance"] }, false],
    ["a matching dag_id", { dag_ids: ["etl_sales", "etl_orders"] }, true],
    ["a non-matching dag_id", { dag_ids: ["etl_orders"] }, false],
  ])("matches on %s", (_label, appliesTo, expected) => {
    expect(matchesAppliesTo(makeView(appliesTo), dagContext)).toBe(expected);
  });

  it("ANDs across keys, requiring every evaluable criterion to match", () => {
    expect(matchesAppliesTo(makeView({ dag_ids: ["etl_sales"], dag_tags: ["ml"] }), dagContext)).toBe(true);
    expect(matchesAppliesTo(makeView({ dag_ids: ["etl_sales"], dag_tags: ["finance"] }), dagContext)).toBe(
      false,
    );
  });

  it("skips criteria the current context cannot evaluate", () => {
    // task_ids is unjudgeable on a Dag-level page, so the dag_tags match decides.
    const view = makeView({ dag_tags: ["ml"], task_ids: ["train_model"] });

    expect(matchesAppliesTo(view, dagContext)).toBe(true);
  });

  it("shows a view when no configured criterion is evaluable at all", () => {
    expect(matchesAppliesTo(makeView({ task_ids: ["train_model"] }), dagContext)).toBe(true);
    expect(matchesAppliesTo(makeView({ dag_ids: ["etl_sales"] }), { isLoading: false })).toBe(true);
  });

  it.each([
    ["task", taskContext],
    ["task instance", taskInstanceContext],
  ])("matches task_ids against the %s in scope", (_label, context) => {
    expect(matchesAppliesTo(makeView({ task_ids: ["train_model"] }), context)).toBe(true);
    expect(matchesAppliesTo(makeView({ task_ids: ["evaluate"] }), context)).toBe(false);
  });

  it.each([
    ["task", taskContext],
    ["task instance", taskInstanceContext],
  ])("matches operators by class name against the %s in scope", (_label, context) => {
    expect(matchesAppliesTo(makeView({ operators: ["KubernetesPodOperator"] }), context)).toBe(true);
    expect(matchesAppliesTo(makeView({ operators: ["PythonOperator"] }), context)).toBe(false);
  });

  it.each([
    [
      "task",
      { dag, isLoading: false, task: makeTask("submit", "SparkSubmitOperator", "Spark Submit") },
    ],
    [
      "task instance",
      {
        dag,
        isLoading: false,
        taskInstance: makeTaskInstance("submit", "SparkSubmitOperator", "Spark Submit"),
      },
    ],
  ])(
    "keeps class name and display name on separate keys for the %s in scope",
    (_label, context: AppliesToContext) => {
      expect(matchesAppliesTo(makeView({ operators: ["SparkSubmitOperator"] }), context)).toBe(true);
      expect(matchesAppliesTo(makeView({ operator_names: ["Spark Submit"] }), context)).toBe(true);
      // Each key sees only its own field, so a display name given to `operators` does not match.
      expect(matchesAppliesTo(makeView({ operators: ["Spark Submit"] }), context)).toBe(false);
      expect(matchesAppliesTo(makeView({ operator_names: ["SparkSubmitOperator"] }), context)).toBe(false);
    },
  );

  it("targets a decorated task by its display name, which is the only name it exposes", () => {
    const context: AppliesToContext = {
      dag,
      isLoading: false,
      taskInstance: makeTaskInstance("run_script", "_BashDecoratedOperator", "@task.bash"),
    };

    expect(matchesAppliesTo(makeView({ operator_names: ["@task.bash"] }), context)).toBe(true);
    expect(matchesAppliesTo(makeView({ operators: ["BashOperator"] }), context)).toBe(false);
  });

  it("combines Dag- and task-level criteria on a task page", () => {
    const view = makeView({ dag_tags: ["ml"], operators: ["KubernetesPodOperator"] });

    expect(matchesAppliesTo(view, taskContext)).toBe(true);
    expect(
      matchesAppliesTo(makeView({ dag_tags: ["finance"], task_ids: ["train_model"] }), taskContext),
    ).toBe(false);
  });
});

describe("isAppliesToPending", () => {
  it("never withholds a view without criteria", () => {
    expect(isAppliesToPending(makeView(), { isLoading: true })).toBe(false);
    expect(isAppliesToPending(makeView({}), { isLoading: true })).toBe(false);
  });

  it("withholds a scoped view while its context is loading", () => {
    expect(isAppliesToPending(makeView({ dag_tags: ["ml"] }), { isLoading: true })).toBe(true);
  });

  it("releases a scoped view once its context has resolved", () => {
    expect(isAppliesToPending(makeView({ dag_tags: ["ml"] }), dagContext)).toBe(false);
  });
});

describe("hasAppliesToCriteria", () => {
  it.each([
    ["no applies_to", undefined, false],
    ["an empty applies_to", {}, false],
    ["only empty criteria lists", { dag_ids: [], operator_names: [] }, false],
    ["dag_tags", { dag_tags: ["ml"] }, true],
    ["dag_ids", { dag_ids: ["etl_sales"] }, true],
    ["task_ids", { task_ids: ["train_model"] }, true],
    ["operators", { operators: ["KubernetesPodOperator"] }, true],
    ["operator_names", { operator_names: ["@task.bash"] }, true],
  ])("reports %s as %s", (_label, appliesTo, expected) => {
    expect(hasAppliesToCriteria(makeView(appliesTo))).toBe(expected);
  });
});
