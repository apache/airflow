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

/* eslint-disable max-lines -- Gantt transform, time range, links, and axis ticks share one module */
import dayjs from "dayjs";
import type { To } from "react-router-dom";

import type { GridRunsResponse, LightGridTaskInstanceSummary, TaskInstanceState } from "openapi/requests";
import type { GanttTaskInstance } from "openapi/requests/types.gen";
import { SearchParamsKeys } from "src/constants/searchParams";
import type { GridTask } from "src/layouts/Details/Grid/utils";
import { isStatePending } from "src/utils";
import { renderDuration } from "src/utils/datetimeUtils";
import { buildTaskInstanceUrl } from "src/utils/links";

export type GanttDataItem = {
  isGroup?: boolean | null;
  isMapped?: boolean | null;
  /** Source try times for tooltips (matches TaskInstance `*_when` fields). */
  queued_when?: string | null;
  scheduled_when?: string | null;
  state?: TaskInstanceState | null;
  taskId: string;
  tryNumber?: number;
  /** [startMs, endMs] as Unix millisecond timestamps — pre-parsed to avoid repeated `new Date()` in render loops. */
  x: [number, number];
  y: string;
};

type GanttSegmentLinkParams = {
  dagId: string;
  item: GanttDataItem;
  /** Precomputed map from taskId → max try number; build once with `buildMaxTryByTaskId`. */
  maxTryByTaskId: Map<string, number>;
  /** `location.pathname` — hoisted out of the segment loop so it is read once per render. */
  pathname: string;
  runId: string;
  /**
   * Pre-parsed copy of `location.search` — pass `new URLSearchParams(location.search)` built
   * once per render. `getGanttSegmentTo` clones it internally before mutating, so the caller's
   * instance is never modified.
   */
  searchParams: URLSearchParams;
};

type TransformGanttDataParams = {
  allTries: Array<GanttTaskInstance>;
  flatNodes: Array<GridTask>;
  gridSummaries: Array<LightGridTaskInstanceSummary>;
};

export const gridSummariesToTaskIdMap = (
  summaries: Array<LightGridTaskInstanceSummary>,
): Map<string, LightGridTaskInstanceSummary> => {
  const byId = new Map<string, LightGridTaskInstanceSummary>();

  for (const summary of summaries) {
    byId.set(summary.task_id, summary);
  }

  return byId;
};

export const transformGanttData = ({
  allTries,
  flatNodes,
  gridSummaries,
}: TransformGanttDataParams): Array<GanttDataItem> => {
  // Pre-index both lookups as Maps to keep the overall transform O(n+m).
  const triesByTask = new Map<string, Array<GanttTaskInstance>>();

  for (const ti of allTries) {
    const existing = triesByTask.get(ti.task_id) ?? [];

    existing.push(ti);
    triesByTask.set(ti.task_id, existing);
  }

  const summaryByTaskId = gridSummariesToTaskIdMap(gridSummaries);

  return flatNodes
    .flatMap((node): Array<GanttDataItem> | undefined => {
      const gridSummary = summaryByTaskId.get(node.id);

      // Groups and mapped tasks show a single aggregate bar sourced from grid summaries.
      if ((node.isGroup ?? node.is_mapped) && gridSummary) {
        if (gridSummary.min_start_date === null || gridSummary.max_end_date === null) {
          return undefined;
        }

        return [
          {
            isGroup: node.isGroup,
            isMapped: node.is_mapped,
            state: gridSummary.state,
            taskId: gridSummary.task_id,
            x: [dayjs(gridSummary.min_start_date).valueOf(), dayjs(gridSummary.max_end_date).valueOf()],
            y: gridSummary.task_id,
          },
        ];
      }

      if (!node.isGroup) {
        const tries = triesByTask.get(node.id);

        if (tries && tries.length > 0) {
          const sortedTries = [...tries].sort(
            (leftTry, rightTry) => leftTry.try_number - rightTry.try_number,
          );

          return sortedTries.flatMap((tryRow: GanttTaskInstance): Array<GanttDataItem> => {
            const items: Array<GanttDataItem> = [];
            const hasTaskRunning = isStatePending(tryRow.state);
            const startDate: string | null = tryRow.start_date;
            const endDate: string | null = tryRow.end_date;
            const queuedDttm = tryRow.queued_dttm;
            const scheduledDttm = tryRow.scheduled_dttm;
            const startMs = startDate === null ? undefined : dayjs(startDate).valueOf();
            const queuedMs = queuedDttm === null ? undefined : dayjs(queuedDttm).valueOf();
            const scheduledMs = scheduledDttm === null ? undefined : dayjs(scheduledDttm).valueOf();

            // Include scheduled/queued times in tooltip data whenever the timestamps exist.
            const tryWhenForTooltip = {
              ...(scheduledMs === undefined ? {} : { scheduled_when: scheduledDttm }),
              ...(queuedMs === undefined ? {} : { queued_when: queuedDttm }),
            };

            let endMs: number;

            if (hasTaskRunning) {
              endMs = Date.now();
            } else if (endDate === null) {
              endMs = startMs ?? Date.now();
            } else {
              endMs = dayjs(endDate).valueOf();
            }

            if (scheduledMs !== undefined) {
              const scheduledEndMs =
                queuedMs ?? startMs ?? (hasTaskRunning || tryRow.state === "scheduled" ? Date.now() : endMs);

              if (scheduledEndMs > scheduledMs) {
                items.push({
                  isGroup: false,
                  isMapped: tryRow.is_mapped,
                  state: "scheduled",
                  taskId: tryRow.task_id,
                  tryNumber: tryRow.try_number,
                  ...tryWhenForTooltip,
                  x: [scheduledMs, scheduledEndMs],
                  y: tryRow.task_display_name,
                });
              }
            }

            if (queuedMs !== undefined) {
              const queueEndMs = startMs ?? (hasTaskRunning ? Date.now() : endMs);

              if (queueEndMs > queuedMs) {
                items.push({
                  isGroup: false,
                  isMapped: tryRow.is_mapped,
                  state: "queued",
                  taskId: tryRow.task_id,
                  tryNumber: tryRow.try_number,
                  ...tryWhenForTooltip,
                  x: [queuedMs, queueEndMs],
                  y: tryRow.task_display_name,
                });
              }
            }

            // Execution segment: start_date → end
            if (startMs !== undefined) {
              const execEndMs = Math.max(startMs, endMs);

              items.push({
                isGroup: false,
                isMapped: tryRow.is_mapped,
                state: tryRow.state,
                taskId: tryRow.task_id,
                tryNumber: tryRow.try_number,
                ...tryWhenForTooltip,
                x: [startMs, execEndMs],
                y: tryRow.task_display_name,
              });
            }

            return items;
          });
        }
      }

      return undefined;
    })
    .filter((item): item is GanttDataItem => item !== undefined);
};

/** One entry per flat node: segments to draw in that row (tries or aggregate). */
export const buildGanttRowSegments = (
  flatNodes: Array<GridTask>,
  items: Array<GanttDataItem>,
): Array<Array<GanttDataItem>> => {
  const byTaskId = new Map<string, Array<GanttDataItem>>();

  for (const item of items) {
    const list = byTaskId.get(item.taskId) ?? [];

    list.push(item);
    byTaskId.set(item.taskId, list);
  }

  return flatNodes.map((node) => byTaskId.get(node.id) ?? []);
};

export const computeGanttTimeRangeMs = ({
  ganttItems,
  selectedRun,
  selectedTimezone,
}: {
  ganttItems: Array<GanttDataItem>;
  selectedRun?: GridRunsResponse;
  selectedTimezone: string;
}): { maxMs: number; minMs: number } => {
  const isActivePending = selectedRun !== undefined && isStatePending(selectedRun.state);
  // Compute the effective end timestamp directly in milliseconds to avoid the
  // string-format → new Date() round-trip which is browser-inconsistent.
  const effectiveEndMs = isActivePending
    ? dayjs().tz(selectedTimezone).valueOf()
    : selectedRun?.end_date !== null && selectedRun?.end_date !== undefined
      ? dayjs(selectedRun.end_date).valueOf()
      : Date.now();

  if (ganttItems.length === 0) {
    const minMs =
      selectedRun?.start_date !== null && selectedRun?.start_date !== undefined
        ? dayjs(selectedRun.start_date).valueOf()
        : Date.now();

    return { maxMs: effectiveEndMs, minMs };
  }

  let minTime = Infinity;
  let maxTime = -Infinity;

  for (const item of ganttItems) {
    if (item.x[0] < minTime) {
      [minTime] = item.x;
    }
    if (item.x[1] > maxTime) {
      [, maxTime] = item.x;
    }
  }

  const totalDuration = maxTime - minTime;

  return {
    maxMs: maxTime + totalDuration * 0.05,
    minMs: minTime - totalDuration * 0.02,
  };
};

/**
 * Precompute the maximum try number for each task in O(n).
 * Pass the result to `getGanttSegmentTo` to avoid an O(n) scan per segment.
 */
export const buildMaxTryByTaskId = (ganttItems: Array<GanttDataItem>): Map<string, number> => {
  const map = new Map<string, number>();

  for (const item of ganttItems) {
    const current = map.get(item.taskId) ?? 0;

    map.set(item.taskId, Math.max(current, item.tryNumber ?? 1));
  }

  return map;
};

export const getGanttSegmentTo = ({
  dagId,
  item,
  maxTryByTaskId,
  pathname,
  runId,
  searchParams: baseSearchParams,
}: GanttSegmentLinkParams): To | undefined => {
  if (!runId) {
    return undefined;
  }

  const { isGroup, isMapped, taskId, tryNumber } = item;

  const segmentPathname = buildTaskInstanceUrl({
    currentPathname: pathname,
    dagId,
    isGroup: Boolean(isGroup),
    isMapped: Boolean(isMapped),
    runId,
    taskId,
  });

  // Clone the pre-parsed params so mutations don't leak across segments.
  const searchParams = new URLSearchParams(baseSearchParams);
  const maxTryForTask = maxTryByTaskId.get(taskId) ?? 1;
  const isOlderTry = tryNumber !== undefined && tryNumber < maxTryForTask;

  if (isOlderTry) {
    searchParams.set(SearchParamsKeys.TRY_NUMBER, tryNumber.toString());
  } else {
    searchParams.delete(SearchParamsKeys.TRY_NUMBER);
  }

  return {
    pathname: segmentPathname,
    search: searchParams.toString(),
  };
};

/** Default number of time labels along the Gantt axis (endpoints included). */
export const GANTT_TIME_AXIS_TICK_COUNT = 8;

export type GanttAxisTickLabelAlign = "center" | "left" | "right";

export type GanttAxisTick = {
  label: string;
  labelAlign: GanttAxisTickLabelAlign;
  leftPct: number;
};

/** Elapsed time from the chart origin (`minMs`), formatted like grid duration labels (no wall-clock). */
const formatElapsedMsForGanttAxis = (elapsedMs: number): string => {
  const seconds = Math.max(0, elapsedMs / 1000);

  if (seconds <= 0.01) {
    return "00:00:00";
  }

  return renderDuration(seconds, false) ?? "00:00:00";
};

export const buildGanttTimeAxisTicks = (
  minMs: number,
  maxMs: number,
  tickCount: number = GANTT_TIME_AXIS_TICK_COUNT,
): Array<GanttAxisTick> => {
  const spanMs = Math.max(1, maxMs - minMs);
  const denominator = Math.max(1, tickCount - 1);
  const lastIndex = tickCount - 1;
  const ticks: Array<GanttAxisTick> = [];

  for (let tickIndex = 0; tickIndex < tickCount; tickIndex += 1) {
    const elapsedMs = (tickIndex / denominator) * spanMs;
    const labelAlign: GanttAxisTickLabelAlign =
      tickCount === 1 ? "left" : tickIndex === 0 ? "left" : tickIndex === lastIndex ? "right" : "center";

    ticks.push({
      label: formatElapsedMsForGanttAxis(elapsedMs),
      labelAlign,
      leftPct: (tickIndex / denominator) * 100,
    });
  }

  return ticks;
};
