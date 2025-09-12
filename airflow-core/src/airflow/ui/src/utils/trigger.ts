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
import dayjs from "dayjs";

import type { ReprocessBehavior } from "openapi/requests/types.gen";

export enum RunMode {
  BACKFILL = "backfill",
  SINGLE = "single",
}

export const getRunModeFromPathname = (pathname: string): RunMode =>
  pathname.endsWith("/trigger/backfill") ? RunMode.BACKFILL : RunMode.SINGLE;

export const normalizeTriggerPath = (pathname: string, runMode: RunMode | null) => {
  const basePath = pathname.replace(/\/?trigger(\/(single|backfill))?$/u, "");

  if (runMode === null) {
    return basePath;
  }

  return `${basePath}/trigger/${runMode}`;
};

const getPreloadTriggerConf = (searchParams: URLSearchParams, reservedParams: Array<string> = []) => {
  const conf = searchParams.get("conf");

  //   if using conf JSON type, we ignore the key=value type params
  if (conf !== null) {
    try {
      const parsed = JSON.parse(decodeURIComponent(conf)) as Record<string, unknown>;

      return JSON.stringify(parsed, null, 2);
    } catch {
      return null;
    }
  }

  //   collect the params not in reservedParams, which means key=value type params
  const collected: Record<string, unknown> = {};

  searchParams.forEach((value, key) => {
    if (!reservedParams.includes(key)) {
      collected[key] = value;
    }
  });

  return Object.keys(collected).length ? JSON.stringify(collected, null, 2) : null;
};

export const getPreloadTriggerFormData = (searchParams: URLSearchParams) => ({
  conf: getPreloadTriggerConf(searchParams, ["run_id", "logical_date", "note"]),
  dagRunId: searchParams.get("run_id") ?? "",
  logicalDate: searchParams.get("logical_date") ?? dayjs().format("YYYY-MM-DDTHH:mm:ss.SSS"),
  note: searchParams.get("note") ?? "",
});

export const getPreloadBackfillFormData = (searchParams: URLSearchParams) => ({
  conf: getPreloadTriggerConf(searchParams, ["max_active_runs", "reprocess_behavior", "run_backwards"]),
  from_date: searchParams.get("start_date") ?? "",
  max_active_runs: parseInt(searchParams.get("max_active_runs") ?? "1", 10),
  reprocess_behavior: (searchParams.get("reprocess_behavior") ?? "none") as ReprocessBehavior,
  run_backwards: ["1", "true"].includes(searchParams.get("run_backwards") ?? ""),
  to_date: searchParams.get("end_date") ?? "",
});
