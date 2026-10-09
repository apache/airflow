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
import { useQueries } from "@tanstack/react-query";

import { UseGridServiceGetLoopSummaryKeyFn } from "openapi/queries/common";
import { GridService } from "openapi/requests/services.gen";
import type { TaskInstanceState } from "openapi/requests/types.gen";

import { useLoopGroupIds } from "src/queries/useIsLoopGroup";

// Loop ids are dotted task group paths, so "::" cannot occur inside one and can join the two
// halves of an option value.
const SEPARATOR = "::";

const elapsedSeconds = (start: string | null | undefined, end: string | null | undefined) =>
  start === null || start === undefined || end === null || end === undefined
    ? undefined
    : (Date.parse(end) - Date.parse(start)) / 1000;

export const encodeLoopOption = (loopId: string, iteration?: number): string =>
  iteration === undefined ? loopId : `${loopId}${SEPARATOR}${iteration}`;

export const decodeLoopOption = (value: string): { iteration?: string; loopId: string } => {
  const index = value.lastIndexOf(SEPARATOR);

  return index === -1
    ? { loopId: value }
    : { iteration: value.slice(index + SEPARATOR.length), loopId: value.slice(0, index) };
};

/**
 * The loops a page should offer: all of them, or just the one a task group is part of.
 *
 * On a group's own page only that group's loop is in view. A group id is a dotted path, so a
 * group nested inside a loop is prefixed by it and reports the loop that encloses it.
 */
export const loopsInScope = (declaredLoops: Array<string>, groupId?: string): Array<string> =>
  groupId === undefined
    ? declaredLoops
    : declaredLoops.filter((id) => id === groupId || groupId.startsWith(`${id}.`));

export type LoopOption = {
  // Set only on the whole-loop entry, which reports the run rather than one pass.
  // Seconds, when the pass has both ends; the accordion header shows the same figure.
  duration: number | undefined;
  iteration: number | undefined;
  iterationsRan: number | undefined;
  loopId: string;
  maxIterations: number | undefined;
  state: TaskInstanceState | undefined;
  taskCount: number | undefined;
  value: string;
};

// A loop's outcome is its own vocabulary, but it lands on the same badge as a task instance so
// the whole-loop entry reads like the passes beneath it. Both terminal-but-unremarkable endings
// -- it converged, or it used up its allowance -- are a loop that did its job.
const LOOP_STATE: Record<string, TaskInstanceState> = {
  failed: "failed",
  ran_to_cap: "success",
  removed: "removed",
  running: "running",
  skipped: "skipped",
  stopped_early: "success",
};

/**
 * Every loop in the run paired with the passes it actually ran, as one flat list.
 *
 * Each loop reports its own iterations, so this fans out over them; a Dag normally declares one.
 */
export const useLoopFilterOptions = ({
  dagId,
  groupId,
  runId,
}: {
  dagId?: string;
  groupId?: string;
  runId?: string;
}) => {
  const loopGroupIds = loopsInScope(useLoopGroupIds(), groupId);
  const enabled = dagId !== undefined && runId !== undefined;

  return useQueries({
    combine: (results) => ({
      isFetching: results.some((result) => result.isFetching),
      options: results.flatMap((result, position): Array<LoopOption> => {
        const loopId = loopGroupIds[position] ?? "";
        // The whole-loop entry stands on its own, so a loop still filters before any
        // iteration has been reported.
        const all: LoopOption = {
          duration: undefined,
          iteration: undefined,
          iterationsRan: result.data?.iterations_ran,
          loopId,
          maxIterations: result.data?.max_iterations,
          state: result.data === undefined ? undefined : LOOP_STATE[result.data.status],
          taskCount: undefined,
          value: encodeLoopOption(loopId),
        };

        return [
          all,
          ...(result.data?.iterations ?? []).map((iteration) => ({
            duration: elapsedSeconds(iteration.start_date, iteration.end_date),
            iteration: iteration.index,
            iterationsRan: undefined,
            loopId,
            maxIterations: undefined,
            state: iteration.state ?? undefined,
            taskCount: iteration.task_count,
            value: encodeLoopOption(loopId, iteration.index),
          })),
        ];
      }),
    }),
    queries: loopGroupIds.map((loopId) => ({
      enabled,
      queryFn: () => GridService.getLoopSummary({ dagId: dagId ?? "", groupId: loopId, runId: runId ?? "" }),
      queryKey: UseGridServiceGetLoopSummaryKeyFn({
        dagId: dagId ?? "",
        groupId: loopId,
        runId: runId ?? "",
      }),
      retry: false,
    })),
  });
};
