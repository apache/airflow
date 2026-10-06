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

import type { UsageInfo } from "src/types/model";

function getApiBase(): string {
  if (typeof document === "undefined") return "/api/v2";
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);
  const basePath = baseUrl.pathname.replace(/\/$/, "") || "";
  return basePath ? `${basePath}/api/v2` : "/api/v2";
}

const API_BASE = getApiBase();

/** Read one XCom entry; returns `null` when the task hasn't pushed that key (404). */
async function fetchXComValue<T>(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
  key: string,
): Promise<T | null> {
  const path =
    `${API_BASE}/dags/${encodeURIComponent(dagId)}` +
    `/dagRuns/${encodeURIComponent(runId)}` +
    `/taskInstances/${encodeURIComponent(taskId)}` +
    `/xcomEntries/${encodeURIComponent(key)}`;
  const res = await fetch(`${path}?map_index=${mapIndex}&deserialize=true`, {
    credentials: "same-origin",
  });
  if (res.status === 404) return null;
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = (body as { detail?: string }).detail;
    throw new Error(detail ?? res.statusText);
  }
  const data = (await res.json()) as { value: T };
  return data.value;
}

export function createModelApi(dagId: string, runId: string, taskId: string, mapIndex: number) {
  return {
    fetchModelName: () => fetchXComValue<string>(dagId, runId, taskId, mapIndex, "model_name"),
    fetchUsage: () => fetchXComValue<UsageInfo>(dagId, runId, taskId, mapIndex, "usage"),
  };
}
