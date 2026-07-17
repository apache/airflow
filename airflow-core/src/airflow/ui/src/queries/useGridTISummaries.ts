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
import { useQueryClient } from "@tanstack/react-query";
import { useEffect, useState } from "react";

import {
  useDagRunServiceGetDagRunsKey,
  useGridServiceGetGridRunsKey,
  useTaskInstanceServiceGetTaskInstancesKey,
} from "openapi/queries";
import type { GridTISummaries, TaskInstanceState } from "openapi/requests";
import { OpenAPI } from "openapi/requests/core/OpenAPI";
import { isStatePending, useAutoRefresh } from "src/utils";

const GRID_MUTATION_WATCHED_KEYS = new Set([
  useTaskInstanceServiceGetTaskInstancesKey,
  useGridServiceGetGridRunsKey,
  useDagRunServiceGetDagRunsKey,
]);

/**
 * Streams TI summaries for all grid runs over a single HTTP connection (NDJSON).
 *
 * The server emits one JSON line per Dag run as soon as that run's task
 * instances have been computed, so the grid renders each column progressively
 * rather than waiting for the entire payload.  This eliminates the N+1 request
 * pattern without loading all runs into one large query.
 *
 * Auto-refreshes while any run is still in a pending state.
 */
export const useGridTiSummariesStream = ({
  dagId,
  runIds,
  states,
}: {
  dagId: string;
  runIds: Array<string>;
  states?: Array<TaskInstanceState | null | undefined>;
}) => {
  const queryClient = useQueryClient();
  const [summariesByRunId, setSummariesByRunId] = useState<Map<string, GridTISummaries>>(new Map());
  const [refreshTick, setRefreshTick] = useState(0);

  const baseRefetchInterval = useAutoRefresh({ dagId });
  const hasActiveRuns = states?.some((state) => state !== undefined && isStatePending(state)) ?? false;

  // Stable key so the effect only re-fires when the run list actually changes.
  const runIdsKey = runIds.join(",");

  // Stream (or re-stream) whenever the run list or refresh tick changes.
  useEffect(() => {
    if (!dagId || runIds.length === 0) {
      return undefined;
    }

    const abortController = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;

    const fetchStream = async () => {
      // Keep stale data visible while the new stream loads — columns update in
      // place as fresh lines arrive rather than flashing blank.
      try {
        const params = new URLSearchParams(runIds.map((id) => ["run_ids", id]));
        const response = await fetch(`${OpenAPI.BASE}/ui/grid/ti_summaries/${dagId}?${params}`, {
          signal: abortController.signal,
        });

        if (!response.ok || !response.body) {
          return;
        }

        reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        // eslint-disable-next-line no-await-in-loop -- sequential reads required; each chunk depends on the previous buffer state
        for (let result = await reader.read(); !result.done; result = await reader.read()) {
          if (abortController.signal.aborted) {
            break;
          }

          const { value } = result;

          buffer += decoder.decode(value, { stream: true });

          const lines = buffer.split("\n");

          buffer = lines.pop() ?? "";

          const newSummaries = lines
            .filter((ln) => ln.trim())
            .map((line) => JSON.parse(line) as GridTISummaries);

          if (newSummaries.length > 0) {
            setSummariesByRunId((prev) => {
              const next = new Map(prev);

              newSummaries.forEach((summary) => next.set(summary.run_id, summary));

              return next;
            });
          }
        }
      } catch (error) {
        if ((error as Error).name !== "AbortError") {
          // eslint-disable-next-line no-console
          console.error("TI summaries stream error:", error);
        }
      }
    };

    void fetchStream();

    return () => {
      abortController.abort();
      if (reader) {
        reader.cancel().catch(() => {
          // Ignore cancellation errors
        });
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- runIdsKey (stable join) intentionally replaces runIds array to avoid spurious re-streams
  }, [dagId, runIdsKey, refreshTick]);

  // Trigger a re-stream periodically while active runs are in flight.
  useEffect(() => {
    if (!hasActiveRuns || typeof baseRefetchInterval !== "number") {
      return undefined;
    }

    // The stream already fetches on mount and whenever runIdsKey changes, so there is no first-interval
    // wait to avoid. Bumping refreshTick here would abort that just-opened mount stream and immediately
    // reopen it — a redundant connection plus an AbortError on every grid mount — so let the interval be
    // the only re-stream trigger.
    const timer = setInterval(() => {
      setRefreshTick((tick) => tick + 1);
    }, baseRefetchInterval);

    return () => clearInterval(timer);
  }, [hasActiveRuns, baseRefetchInterval]);

  // Re-stream whenever a mutation invalidates a grid-related query (TI states,
  // run states, or grid structure).  Invalidation events only fire from explicit
  // invalidateQueries() calls — never from polling intervals — so this never
  // double-fires with the interval-based refresh above.
  useEffect(() => {
    let scheduleScheduled = false;
    let isMounted = true;

    const schedule =
      typeof globalThis.queueMicrotask === "function"
        ? globalThis.queueMicrotask
        : (cb: () => void) => {
            setTimeout(cb, 0);
          };

    const runScheduledRefresh = () => {
      if (isMounted) {
        setRefreshTick((tick) => tick + 1);
      }
      scheduleScheduled = false;
    };

    const unsubscribe = queryClient.getQueryCache().subscribe((event) => {
      const [firstKey] = event.query.queryKey as Array<unknown>;

      if (
        event.type === "updated" &&
        event.action.type === "invalidate" &&
        typeof firstKey === "string" &&
        GRID_MUTATION_WATCHED_KEYS.has(firstKey)
      ) {
        // Coalesce: multiple invalidations in the same execution tick only trigger one re-stream.
        if (!scheduleScheduled) {
          scheduleScheduled = true;
          schedule(runScheduledRefresh);
        }
      }
    });

    return () => {
      isMounted = false;
      unsubscribe();
    };
  }, [queryClient]);

  return { summariesByRunId };
};
