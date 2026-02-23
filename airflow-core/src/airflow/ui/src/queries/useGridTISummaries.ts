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
import { useEffect, useRef, useState } from "react";

import { useGridServiceGetGridTiSummaries } from "openapi/queries";
import { OpenAPI } from "openapi/requests/core/OpenAPI";
import type { GridTISummaries, TaskInstanceState } from "openapi/requests";
import { isStatePending, useAutoRefresh } from "src/utils";

export const useGridTiSummaries = ({
  dagId,
  enabled,
  isSelected,
  runId,
  state,
}: {
  dagId: string;
  enabled?: boolean;
  isSelected?: boolean;
  runId: string;
  state?: TaskInstanceState | null | undefined;
}) => {
  const baseRefetchInterval = useAutoRefresh({ dagId });
  const slowRefreshMultiplier = 5;
  const refetchInterval =
    typeof baseRefetchInterval === "number"
      ? baseRefetchInterval * (isSelected ? 1 : slowRefreshMultiplier)
      : baseRefetchInterval;

  const { data: gridTiSummaries, ...rest } = useGridServiceGetGridTiSummaries(
    {
      dagId,
      runId,
    },
    undefined,
    {
      enabled: Boolean(runId) && Boolean(dagId) && enabled,
      placeholderData: (prev) => prev,
      refetchInterval: (query) =>
        ((state !== undefined && isStatePending(state)) ||
          query.state.data?.task_instances.some((ti) => isStatePending(ti.state))) &&
        refetchInterval,
    },
  );

  return { data: gridTiSummaries, ...rest };
};

/**
 * Streams TI summaries for all grid runs over a single HTTP connection (NDJSON).
 *
 * The server emits one JSON line per DAG run as soon as that run's task
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
  const [summariesByRunId, setSummariesByRunId] = useState<Map<string, GridTISummaries>>(new Map());
  const [isStreaming, setIsStreaming] = useState(false);
  const [refreshTick, setRefreshTick] = useState(0);
  // Mutated during render (not inside an effect) so the per-run fallback in
  // TaskInstancesColumn is suppressed on the very same render that runIds
  // first becomes non-empty — before useEffect has a chance to fire.
  const hasRunsRef = useRef(false);

  if (runIds.length > 0) {
    hasRunsRef.current = true;
  }

  const baseRefetchInterval = useAutoRefresh({ dagId });
  const hasActiveRuns = states?.some((s) => s !== undefined && isStatePending(s)) ?? false;

  // Stable key so the effect only re-fires when the run list actually changes.
  const runIdsKey = runIds.join(",");

  // Stream (or re-stream) whenever the run list or refresh tick changes.
  useEffect(() => {
    if (!dagId || runIds.length === 0) return;

    const abortController = new AbortController();
    let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;

    const fetchStream = async () => {
      setIsStreaming(true);
      // Keep stale data visible while the new stream loads — columns update in
      // place as fresh lines arrive rather than flashing blank.
      try {
        const queryString = runIds.map((id) => `run_ids=${encodeURIComponent(id)}`).join("&");
        const response = await fetch(`${OpenAPI.BASE}/ui/grid/ti_summaries/${dagId}?${queryString}`, {
          signal: abortController.signal,
        });

        if (!response.ok || !response.body) return;

        reader = response.body.getReader();
        const decoder = new TextDecoder();
        let buffer = "";

        while (true) {
          const { done, value } = await reader.read();

          if (done) break;
          buffer += decoder.decode(value, { stream: true });

          // Each complete line is one serialised GridTISummaries object.
          const lines = buffer.split("\n");
          buffer = lines.pop() ?? "";

          for (const line of lines) {
            if (line.trim()) {
              const summary = JSON.parse(line) as GridTISummaries;

              setSummariesByRunId((prev) => new Map(prev).set(summary.run_id, summary));
            }
          }
        }
      } catch (error) {
        if ((error as Error).name !== "AbortError") {
          console.error("TI summaries stream error:", error);
        }
      } finally {
        setIsStreaming(false);
      }
    };

    fetchStream();

    return () => {
      abortController.abort();
      reader?.cancel();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [dagId, runIdsKey, refreshTick]);

  // Trigger a re-stream periodically while active runs are in flight.
  useEffect(() => {
    if (!hasActiveRuns || typeof baseRefetchInterval !== "number") return;

    const timer = setInterval(() => {
      setRefreshTick((t) => t + 1);
    }, baseRefetchInterval);

    return () => clearInterval(timer);
  }, [hasActiveRuns, baseRefetchInterval]);

  return { isStreaming: isStreaming || hasRunsRef.current, summariesByRunId };
};
