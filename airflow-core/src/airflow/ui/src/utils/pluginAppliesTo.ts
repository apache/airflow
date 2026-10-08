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
  DAGRunResponse,
  ExternalViewResponse,
  ReactAppResponse,
  TaskInstanceResponse,
  TaskResponse,
} from "openapi/requests/types.gen";

export type PluginView = ExternalViewResponse | ReactAppResponse;

/**
 * The records the current route resolved to, which `applies_to` paths are evaluated against.
 *
 * A field is `undefined` when the route has no such record (e.g. no `task` on a Dag-level
 * page), which makes every path rooted at it unevaluable. `isLoading` is true while a record
 * the route *does* have is still being fetched.
 */
export type AppliesToContext = {
  dag?: DAGResponse;
  dagRun?: DAGRunResponse;
  isLoading: boolean;
  task?: TaskResponse;
  taskInstance?: TaskInstanceResponse;
};

type RootName = "dag" | "dagRun" | "task" | "taskInstance";

// A path may name a related record as its first segment. Keyed by the wire spelling, since
// that is what a plugin author writes.
const ROOT_BY_PREFIX: Record<string, RootName> = {
  dag: "dag",
  dag_run: "dagRun",
  task: "task",
  task_instance: "taskInstance",
};

// Which record an unqualified path is rooted at: the entity the destination is about. A
// destination missing here can evaluate nothing, so all of its paths are skipped.
// Kept in sync with `_APPLIES_TO_ENTITY_ROOT` in `airflow/plugins_manager.py`.
const ENTITY_ROOT_BY_DESTINATION: Record<string, RootName> = {
  dag: "dag",
  dag_overview: "dag",
  dag_run: "dagRun",
  task: "task",
  task_instance: "taskInstance",
  task_overview: "task",
};

type Resolution = { found: false } | { found: true; values: Array<unknown> };

/**
 * Walk a dotted path through the context's records.
 *
 * Traversing a list fans out across its elements, so `dag.tags.name` collects every tag name --
 * the general form of the hardcoded `dag.tags.some(...)` this replaces.
 */
const resolvePath = (
  path: string,
  destination: string | undefined,
  context: AppliesToContext,
): Resolution => {
  const segments = path.split(".");
  const [head, ...tail] = segments;
  const prefixRoot = head === undefined ? undefined : ROOT_BY_PREFIX[head];
  const isQualified = prefixRoot !== undefined && tail.length > 0;

  const rootName = isQualified
    ? prefixRoot
    : destination === undefined
      ? undefined
      : ENTITY_ROOT_BY_DESTINATION[destination];
  const root = rootName === undefined ? undefined : context[rootName];

  if (root === undefined) {
    return { found: false };
  }

  let nodes: Array<unknown> = [root];

  for (const segment of isQualified ? tail : segments) {
    if (nodes.length === 0) {
      // The previous segment was present but fanned out to nothing -- a Dag with no tags, say.
      // That is a definite "no values to match", not a path the page cannot judge.
      return { found: true, values: [] };
    }

    // `Object.hasOwn`, not `in`: `in` walks the prototype chain, so a path segment naming an
    // inherited member (`constructor`, `toString`) would read as a field the record has.
    const readable = nodes.filter(
      (node): node is Record<string, unknown> =>
        node !== null && typeof node === "object" && Object.hasOwn(node, segment),
    );

    if (readable.length === 0) {
      // The record has no such field. This is indistinguishable at runtime from an author
      // typo, so it has to be treated as unevaluable: `operator` exists on a task instance but
      // not on a task, and a block naming both sources must still work on both pages. The
      // cost is that a typo widens the scope rather than narrowing it.
      return { found: false };
    }

    nodes = readable.flatMap((node) => {
      const value = node[segment];

      return Array.isArray(value) ? (value as Array<unknown>) : [value];
    });
  }

  return { found: true, values: nodes };
};

// Stringified so a numeric field (`map_index`, `try_number`) or a boolean (`is_paused`) is
// addressable without the author having to think about JSON types. Anything that is not a
// primitive has no sensible string form: a path stopping on an object or `null` yields no
// comparable value, so it reads as "no match" rather than as unevaluable. Stopping a path short
// of a leaf therefore hides the view instead of widening it -- the opposite of a bad segment.
const toComparable = (value: unknown): string | undefined => {
  if (typeof value === "string") {
    return value;
  }

  return typeof value === "boolean" || typeof value === "number" ? String(value) : undefined;
};

// `undefined` means the page cannot judge this path, which is distinct from `false` (records
// available, nothing matched).
const matchesPath = ({
  allowed,
  context,
  destination,
  path,
}: {
  allowed: Array<string>;
  context: AppliesToContext;
  destination: string | undefined;
  path: string;
}): boolean | undefined => {
  const resolution = resolvePath(path, destination, context);

  if (!resolution.found) {
    return undefined;
  }

  return resolution.values.some((value) => {
    const comparable = toComparable(value);

    return comparable !== undefined && allowed.includes(comparable);
  });
};

const isNonEmpty = (value: Array<string> | null | undefined): value is Array<string> =>
  value !== undefined && value !== null && value.length > 0;

/**
 * Decide whether a plugin view should be shown for the current route.
 *
 * Values are OR-ed within a path and AND-ed across paths, but only across paths whose root
 * record the current destination actually has -- a `task_instance.*` path cannot be judged on a
 * Dag-level page, so it is skipped there rather than failing the match. That is what lets one
 * `applies_to` block be shared by a plugin's Dag- and task-level destinations.
 *
 * Omitting `applies_to` (or giving it no paths) shows the view everywhere.
 */
export const matchesAppliesTo = (view: PluginView, context: AppliesToContext): boolean => {
  const { applies_to: appliesTo } = view;

  if (appliesTo === undefined || appliesTo === null) {
    return true;
  }

  const verdicts = Object.entries(appliesTo)
    .filter(([, allowed]) => isNonEmpty(allowed))
    .map(([path, allowed]) => matchesPath({ allowed, context, destination: view.destination, path }))
    .filter((verdict) => verdict !== undefined);

  // An empty list means no path was evaluable (either none configured, or none judgeable
  // here), and `every` is vacuously true for it -- so the view shows, which is the default.
  return verdicts.every(Boolean);
};

/**
 * Whether a view configures any scoping at all.
 *
 * Callers use this to skip fetching the context records entirely when no view needs them.
 */
export const hasAppliesToCriteria = (view: PluginView): boolean => {
  const { applies_to: appliesTo } = view;

  return appliesTo !== undefined && appliesTo !== null && Object.values(appliesTo).some(isNonEmpty);
};

/**
 * Whether a view should be withheld while the records its paths need are in flight.
 *
 * Without this, a scoped view would render on first paint and disappear once the queries
 * resolve. Unscoped views never wait.
 */
export const isAppliesToPending = (view: PluginView, context: AppliesToContext): boolean =>
  hasAppliesToCriteria(view) && context.isLoading;
