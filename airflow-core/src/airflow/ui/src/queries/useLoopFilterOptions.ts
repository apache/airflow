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

import { useDagRunServiceGetDagRun } from "openapi/queries";
import { UseGridServiceGetLoopSummaryKeyFn } from "openapi/queries/common";
import { GridService } from "openapi/requests/services.gen";
import type { TaskInstanceState } from "openapi/requests/types.gen";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useLoopGroupIds } from "src/queries/useIsLoopGroup";
import { isStatePending, useAutoRefresh } from "src/utils";

// Loop ids are dotted task group paths, so "::" cannot occur inside one and can join the two
// halves of an option value.
const SEPARATOR = "::";

export const encodeLoopOption = (loopId: string, iteration?: number): string =>
  iteration === undefined ? loopId : `${loopId}${SEPARATOR}${iteration}`;

export const decodeLoopOption = (value: string): { iteration?: string; loopId: string } => {
  const index = value.lastIndexOf(SEPARATOR);

  return index === -1
    ? { loopId: value }
    : { iteration: value.slice(index + SEPARATOR.length), loopId: value.slice(0, index) };
};

/**
 * The pass a page is showing, given the loop's options and the url that addressed it.
 *
 * A table carries the pass as a filter and a task instance as a region coordinate; either names
 * the same pass. A pass belongs to the loop that counted it, so one carried over from a different
 * loop is ignored rather than read as a position in this one. With nothing pinned the newest
 * pass is what the page means, since that is the one being read.
 */
export const selectActivePass = (
  options: Array<LoopOption>,
  searchParams: URLSearchParams,
  resolvedPass?: number | null,
): LoopOption | undefined => {
  const [, ...passes] = options;

  // The server resolves a row's pass for us, and it is the only answer that holds when the row
  // is a mapped expansion -- there the region index is the map slot, not the pass -- or when a
  // clear forked the later passes into a successor region.
  if (resolvedPass !== null && resolvedPass !== undefined) {
    return passes.find((pass) => pass.iteration === resolvedPass);
  }

  const carriedLoop = searchParams.get(SearchParamsKeys.LOOP_ID);

  if (carriedLoop !== null && carriedLoop !== "" && carriedLoop !== passes[0]?.loopId) {
    return passes[0];
  }

  const pinned = searchParams.get(SearchParamsKeys.ITERATION);

  return pinned === null || pinned === ""
    ? passes[0]
    : passes.find((pass) => String(pass.iteration) === pinned);
};

export type LoopOption = {
  // Set only on the whole-loop entry, which reports the run rather than one pass.
  iteration: number | undefined;
  iterationsRan: number | undefined;
  loopId: string;
  // The loop's own region. With a pass index it addresses that pass's row exactly, which is
  // what a task instance page needs to show the pass rather than merely filter by it.
  loopRegionId: string | undefined;
  maxIterations: number | undefined;
  state: TaskInstanceState | undefined;
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
  const loopGroupIds = useLoopGroupIds(groupId);
  const enabled = dagId !== undefined && runId !== undefined;
  const refetchInterval = useAutoRefresh({ dagId });
  const { data: dagRun } = useDagRunServiceGetDagRun(
    { dagId: dagId ?? "", dagRunId: runId ?? "" },
    undefined,
    { enabled },
  );
  const isRunPending = isStatePending(dagRun?.state);

  return useQueries({
    combine: (results) => ({
      isFetching: results.some((result) => result.isFetching),
      options: results.flatMap((result, position): Array<LoopOption> => {
        const loopId = loopGroupIds[position] ?? "";
        // The whole-loop entry stands on its own, so a loop still filters before any
        // iteration has been reported.
        const all: LoopOption = {
          iteration: undefined,
          iterationsRan: result.data?.iterations_ran,
          loopId,
          loopRegionId: result.data?.loop_region_id ?? undefined,
          maxIterations: result.data?.max_iterations,
          state: result.data === undefined ? undefined : LOOP_STATE[result.data.status],
          value: encodeLoopOption(loopId),
        };

        return [
          all,
          // The service reports passes oldest-first, but the newest is the one being read, so
          // it leads the list and sits first after the whole-loop entry.
          ...[...(result.data?.iterations ?? [])].reverse().map((iteration) => ({
            iteration: iteration.index,
            iterationsRan: undefined,
            loopId,
            loopRegionId: result.data?.loop_region_id ?? undefined,
            maxIterations: undefined,
            state: iteration.state ?? undefined,
            value: encodeLoopOption(loopId, iteration.index),
          })),
        ];
      }),
    }),
    // A pass belongs to one run, so with no run in scope there is nothing to offer. Without this
    // the whole-loop entry is still built from an unresolved query, and callers that key off
    // ``options.length`` -- the breadcrumb's iteration level among them -- would show it anyway.
    queries: (enabled ? loopGroupIds : []).map((loopId) => ({
      enabled,
      queryFn: () => GridService.getLoopSummary({ dagId: dagId ?? "", groupId: loopId, runId: runId ?? "" }),
      queryKey: UseGridServiceGetLoopSummaryKeyFn({
        dagId: dagId ?? "",
        groupId: loopId,
        runId: runId ?? "",
      }),
      // A running loop gains passes and changes their states, so the switcher, breadcrumb and
      // filter options follow the run the way the loop summary does. Polling stops as soon as
      // the run is terminal.
      refetchInterval: isRunPending && refetchInterval,
      retry: false,
    })),
  });
};
