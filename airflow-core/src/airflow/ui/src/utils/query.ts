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

  const canRefresh = autoRefreshInterval !== undefined && !paused;

  // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-assertion
  return (canRefresh ? autoRefreshInterval * 1000 : false) as number | false;
};

export const decodeParam = (params: URLSearchParams, key: string): string | null => {
  const value = params.get(key);

  if (value === null) {
    return null;
  }
  try {
    return decodeURIComponent(value);
  } catch {
    // Return original value if decoding fails
    return value;
  }
};

export const parseJsonSafe = (raw: string | null): string | null => {
  if (raw === null) {
    return null;
  }
  try {
    const parsed: unknown = JSON.parse(raw);

    return JSON.stringify(parsed, null, 2);
  } catch {
    // Return original string if parsing fails
    return raw;
  }
};

export const getUrlParam = (
  params: URLSearchParams,
  key: string,
  parseAsJson: boolean = false,
): string | null => {
  const decoded = decodeParam(params, key);

  if (parseAsJson && decoded !== null && decoded.trim().length > 0) {
    return parseJsonSafe(decoded);
  }

  return decoded;
};
