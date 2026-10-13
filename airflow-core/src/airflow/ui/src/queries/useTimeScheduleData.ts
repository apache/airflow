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
import { experimental_streamedQuery as streamedQuery, useQuery } from "@tanstack/react-query";

import { OpenAPI } from "openapi/requests/core/OpenAPI";
import type { TimeScheduleBatch } from "openapi/requests/types.gen";

import { useAutoRefresh } from "src/utils";

const streamTimeSchedule = async function* (streamQuery: string, signal: AbortSignal) {
  const response = await fetch(`${OpenAPI.BASE}/ui/time-schedule?${streamQuery}`, { signal });

  if (!response.ok || !response.body) {
    throw new Error(`Time Schedule request failed with status ${response.status}`);
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  const cancelReader = () => {
    void reader.cancel().catch(() => undefined);
  };

  signal.addEventListener("abort", cancelReader, { once: true });
  let buffer = "";

  try {
    while (!signal.aborted) {
      // Chunks share a buffer, so reads must remain sequential.
      // eslint-disable-next-line no-await-in-loop
      const { done, value } = await reader.read();

      signal.throwIfAborted();
      buffer += done ? decoder.decode() : decoder.decode(value, { stream: true });
      const lines = buffer.split("\n");

      buffer = lines.pop() ?? "";
      if (done && buffer.trim()) {
        lines.push(buffer);
      }
      yield* lines.filter((line) => line.trim()).map((line) => JSON.parse(line) as TimeScheduleBatch);
      if (done) {
        break;
      }
    }
  } finally {
    signal.removeEventListener("abort", cancelReader);
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
};

export const useTimeScheduleData = (streamQuery: string) => {
  const refetchInterval = useAutoRefresh({});
  const nonZoomQuery = new URLSearchParams(streamQuery);

  nonZoomQuery.delete("time_scale");
  const nonZoomStreamQuery = nonZoomQuery.toString();
  const query = useQuery({
    placeholderData: (previousData, previousQuery) =>
      previousQuery?.queryKey[1] === nonZoomStreamQuery ? previousData : undefined,
    queryFn: streamedQuery<TimeScheduleBatch>({
      refetchMode: "replace",
      streamFn: ({ signal }) => streamTimeSchedule(streamQuery, signal),
    }),
    queryKey: ["time-schedule", nonZoomStreamQuery, streamQuery],
    refetchInterval,
    refetchIntervalInBackground: false,
    retry: false,
  });

  return {
    dagRunCount: query.data?.reduce((count, batch) => count + batch.dag_run_count, 0) ?? 0,
    error: query.error,
    isLoading: query.isPending,
    timelineItems: query.data?.flatMap((batch) => batch.items) ?? [],
  };
};
