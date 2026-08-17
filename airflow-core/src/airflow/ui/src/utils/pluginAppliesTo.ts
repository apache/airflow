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
import type {
  DAGResponse,
  ExternalViewResponse,
  ReactAppResponse,
  TaskInstanceResponse,
  TaskResponse,
} from "openapi/requests/types.gen";

export type PluginView = ExternalViewResponse | ReactAppResponse;

/**
 * The records the current route resolved to, used to evaluate `applies_to`.
 *
 * A field is `undefined` when the current destination has no such record (e.g. no
 * `task` on a Dag-level page). `isLoading` is true while a record the route *does*
 * have is still being fetched.
 */
export type AppliesToContext = {
  dag?: DAGResponse;
  isLoading: boolean;
  task?: TaskResponse;
  taskInstance?: TaskInstanceResponse;
};

const isNonEmpty = (value: Array<string> | null | undefined): value is Array<string> =>
  value !== undefined && value !== null && value.length > 0;

const knownNames = (...values: Array<string | null | undefined>): Array<string> =>
  values.filter((value): value is string => value !== undefined && value !== null && value !== "");

const matchesAnyName = (criteria: Array<string>, names: Array<string>): boolean | undefined =>
  names.length === 0 ? undefined : names.some((name) => criteria.includes(name));

// A criterion resolves to `undefined` when the current context cannot judge it, which
// is distinct from `false` (context available, nothing matched).
const matchesDagTags = (criteria: Array<string>, { dag }: AppliesToContext): boolean | undefined =>
  dag === undefined ? undefined : dag.tags.some((tag) => criteria.includes(tag.name));

const matchesDagIds = (criteria: Array<string>, { dag }: AppliesToContext): boolean | undefined =>
  dag === undefined ? undefined : criteria.includes(dag.dag_id);

const matchesTaskIds = (
  criteria: Array<string>,
  { task, taskInstance }: AppliesToContext,
): boolean | undefined => {
  const taskId = taskInstance?.task_id ?? task?.task_id;

  return taskId === undefined || taskId === null ? undefined : criteria.includes(taskId);
};

// Matched separately, the way the task instance filters are: the class name comes from
// `task_type` and the display name from `custom_operator_name`. A decorated task
// (`@task.bash`) is reachable only by display name, since its class name is private.
const matchesOperators = (
  criteria: Array<string>,
  { task, taskInstance }: AppliesToContext,
): boolean | undefined => {
  if (taskInstance !== undefined) {
    return matchesAnyName(criteria, knownNames(taskInstance.operator));
  }

  const classRef = task?.class_ref as { class_name?: string } | null | undefined;

  return matchesAnyName(criteria, knownNames(classRef?.class_name));
};

const matchesOperatorNames = (
  criteria: Array<string>,
  { task, taskInstance }: AppliesToContext,
): boolean | undefined => {
  if (taskInstance !== undefined) {
    return matchesAnyName(criteria, knownNames(taskInstance.operator_name));
  }

  return matchesAnyName(criteria, knownNames(task?.operator_name));
};

/**
 * Decide whether a plugin view should be shown for the current route.
 *
 * Criteria are OR-ed within a key and AND-ed across keys, but only across keys the
 * current destination can actually evaluate — a `task_ids` criterion cannot be judged
 * on a Dag-level page, so it is skipped there rather than failing the match. This lets
 * one `applies_to` block be shared by a plugin's Dag- and task-level destinations.
 *
 * Omitting `applies_to` (or giving it no criteria) shows the view everywhere.
 */
export const matchesAppliesTo = (view: PluginView, context: AppliesToContext): boolean => {
  const { applies_to: appliesTo } = view;

  if (appliesTo === undefined || appliesTo === null) {
    return true;
  }

  const {
    dag_ids: dagIds,
    dag_tags: dagTags,
    operator_names: operatorNames,
    operators,
    task_ids: taskIds,
  } = appliesTo;

  const verdicts = [
    isNonEmpty(dagTags) ? matchesDagTags(dagTags, context) : undefined,
    isNonEmpty(dagIds) ? matchesDagIds(dagIds, context) : undefined,
    isNonEmpty(taskIds) ? matchesTaskIds(taskIds, context) : undefined,
    isNonEmpty(operators) ? matchesOperators(operators, context) : undefined,
    isNonEmpty(operatorNames) ? matchesOperatorNames(operatorNames, context) : undefined,
  ].filter((verdict) => verdict !== undefined);

  // No criterion was evaluable (either none configured, or none judgeable here).
  if (verdicts.length === 0) {
    return true;
  }

  return verdicts.every(Boolean);
};

/**
 * Whether a view configures any scoping criterion at all.
 *
 * Callers use this to skip fetching the context records entirely when no view needs them.
 */
export const hasAppliesToCriteria = (view: PluginView): boolean => {
  const { applies_to: appliesTo } = view;

  return appliesTo !== undefined && appliesTo !== null && Object.values(appliesTo).some(isNonEmpty);
};

/**
 * Whether a view should be withheld while the records its criteria need are in flight.
 *
 * Without this, a scoped view would render on first paint and disappear once the
 * queries resolve. Unscoped views never wait.
 */
export const isAppliesToPending = (view: PluginView, context: AppliesToContext): boolean =>
  hasAppliesToCriteria(view) && context.isLoading;
