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
import type { Query } from "@tanstack/react-query";

import { useDagServiceGetDagDetails } from "openapi/queries";
import type { TaskInstanceState } from "openapi/requests/types.gen";
import { useConfig } from "src/queries/useConfig";

export const isStatePending = (state?: TaskInstanceState | null) =>
  state === "deferred" ||
  state === "scheduled" ||
  state === "running" ||
  state === "up_for_reschedule" ||
  state === "up_for_retry" ||
  state === "queued" ||
  state === "restarting" ||
  !Boolean(state);

export type PartialQueryKey = { baseKey: string; options?: Record<string, unknown> };

// This allows us to specify what query key values we actually care about and ignore the rest
// ex: match everything with this dagId and dagRunId but ignore anything related to pagination
export const doQueryKeysMatch = (query: Query, queryKeysToMatch: Array<PartialQueryKey>) => {
  const [baseKey, options] = query.queryKey;

  const matchedKey = queryKeysToMatch.find((qk) => qk.baseKey === baseKey);

  if (!matchedKey) {
    return false;
  }

  return matchedKey.options
    ? Object.entries(matchedKey.options).every(
        ([key, value]) => typeof options === "object" && (options as Record<string, unknown>)[key] === value,
      )
    : true;
};

export const useAutoRefresh = ({ dagId, isPaused }: { dagId?: string; isPaused?: boolean }) => {
  const autoRefreshInterval = useConfig("auto_refresh_interval") as number | undefined;
  const { data: dag } = useDagServiceGetDagDetails(
    {
      dagId: dagId ?? "",
    },
    undefined,
    { enabled: dagId !== undefined },
  );

  const paused = isPaused ?? dag?.is_paused;

  const canRefresh = autoRefreshInterval !== undefined && (dagId === undefined ? true : !paused);

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  return (canRefresh ? autoRefreshInterval * 1000 : false) as number | false;
};
