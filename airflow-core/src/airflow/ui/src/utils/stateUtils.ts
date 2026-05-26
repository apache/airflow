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
import type { TaskInstanceState } from "openapi/requests/types.gen";

/**
 * Priority ordering of task instance states for visual display in the
 * segmented state bar and the tooltip per-state breakdown.  Failure/error
 * states first, then active states (running, deferred), then pending states
 * (queued, scheduled), then terminal states (success, skipped, removed).
 * Unknown keys — including the serialized no-status key `"None"` — sort to
 * the end so the existing tokenless / untranslated rendering for that key
 * (tracked at https://github.com/apache/airflow/issues/67541) appears in the
 * least prominent position until the render sites are normalized.
 *
 * Deliberately diverges from `state_priority` in
 * `api_fastapi/common/parameters.py`, which puts QUEUED/SCHEDULED ahead of
 * RUNNING/DEFERRED. The UI prefers showing the most active state present so a
 * group with running children doesn't render as queued-colored.
 */
export const STATE_PRIORITY: Array<string> = [
  "failed",
  "upstream_failed",
  "up_for_retry",
  "up_for_reschedule",
  "running",
  "restarting",
  "deferred",
  "queued",
  "scheduled",
  "success",
  "skipped",
  "removed",
];

/**
 * Priority ordering used only by `getDisplayState` to pick the dominant state
 * for badge / border / icon / MiniMap coloring.  Differs from
 * `STATE_PRIORITY` by including the serialized no-status key `"None"` between
 * the pending and terminal blocks, matching the backend's `state_priority`
 * placement of Python `None` (see `api_fastapi/common/parameters.py`).  Kept
 * separate from `STATE_PRIORITY` so the segmented bar and tooltip breakdown
 * — which iterate the raw key — do not visually promote the broken
 * `"None"` rendering ahead of terminal slices.
 */
const DOMINANT_STATE_PRIORITY: Array<string> = [
  "failed",
  "upstream_failed",
  "up_for_retry",
  "up_for_reschedule",
  "running",
  "restarting",
  "deferred",
  "queued",
  "scheduled",
  "None",
  "success",
  "skipped",
  "removed",
];

/**
 * Sort child_states entries by priority (highest priority first) and filter out
 * entries with zero counts.  Unknown states are sorted to the end.
 */
export const sortStateEntries = (
  childStates: Record<string, number> | null | undefined,
): Array<[string, number]> => {
  if (!childStates) {
    return [];
  }

  return Object.entries(childStates)
    .filter(([, count]) => count > 0)
    .sort(([stateA], [stateB]) => {
      const idxA = STATE_PRIORITY.indexOf(stateA);
      const idxB = STATE_PRIORITY.indexOf(stateB);
      const priorityA = idxA === -1 ? STATE_PRIORITY.length : idxA;
      const priorityB = idxB === -1 ? STATE_PRIORITY.length : idxB;

      return priorityA - priorityB;
    });
};

/**
 * Pick the state to use for visual coloring on a task instance summary.  For
 * groups and mapped tasks, returns the highest-priority state present in
 * `child_states` (per `DOMINANT_STATE_PRIORITY`) so the badge / border / icon
 * reflect the most active state rather than the backend's `agg_state` result,
 * which puts queued/scheduled ahead of running/deferred.  Falls back to
 * `fallbackState` when `child_states` is empty or absent (individual tasks).
 *
 * When the dominant key is `"None"` (serialized Python `None` — e.g. mapped
 * tasks without rows yet, or no-status children), returns `null` so callers
 * render the existing no-status placeholder rather than casting the literal
 * string `"None"` into Chakra tokens / translation keys.
 */
export const getDisplayState = (
  childStates: Record<string, number> | null | undefined,
  fallbackState: TaskInstanceState | null | undefined,
): TaskInstanceState | null | undefined => {
  if (!childStates) {
    return fallbackState;
  }

  const sorted = Object.entries(childStates)
    .filter(([, count]) => count > 0)
    .sort(([stateA], [stateB]) => {
      const idxA = DOMINANT_STATE_PRIORITY.indexOf(stateA);
      const idxB = DOMINANT_STATE_PRIORITY.indexOf(stateB);
      const priorityA = idxA === -1 ? DOMINANT_STATE_PRIORITY.length : idxA;
      const priorityB = idxB === -1 ? DOMINANT_STATE_PRIORITY.length : idxB;

      return priorityA - priorityB;
    });
  const dominant = sorted[0]?.[0];

  if (dominant === undefined) {
    return fallbackState;
  }
  if (dominant === "None") {
    return null;
  }

  return dominant as TaskInstanceState;
};
