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

import type { PaginatedResult, RuleHistoryRecord, TaskDQRunRecord } from "src/types/dq";

function getBase(): string {
  if (typeof document === "undefined") return "/dataquality";
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);
  const basePath = baseUrl.pathname.replace(/\/$/, "") || "";
  return basePath ? `${basePath}/dataquality` : "/dataquality";
}

const BASE = getBase();

export class ApiError extends Error {
  status: number;

  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const body: unknown = await res.json().catch(() => ({}));
    const detail = (body as { detail?: string })?.detail;
    throw new ApiError(detail ?? res.statusText, res.status);
  }
  return res.json() as Promise<T>;
}

interface PageOptions {
  before?: string | null;
  limit?: number;
}

function buildPageQuery({ before, limit }: PageOptions): string {
  const params = new URLSearchParams();
  if (limit !== undefined) params.set("limit", String(limit));
  if (before) params.set("before", before);
  const query = params.toString();
  return query ? `?${query}` : "";
}

export function createApi(dagId: string, taskId: string, runId: string, mapIndex: number) {
  return {
    fetchRuleHistory: (ruleUid: string, options: PageOptions = {}) =>
      apiFetch<PaginatedResult<RuleHistoryRecord>>(
        `/v1/dags/${encodeURIComponent(dagId)}/tasks/${encodeURIComponent(taskId)}` +
          `/rules/${encodeURIComponent(ruleUid)}/history${buildPageQuery({ limit: 100, ...options })}`,
      ),
    fetchTaskRuns: (options: PageOptions = {}) =>
      apiFetch<PaginatedResult<TaskDQRunRecord>>(
        `/v1/dags/${encodeURIComponent(dagId)}/tasks/${encodeURIComponent(taskId)}/runs` +
          buildPageQuery({ limit: 50, ...options }),
      ),
    fetchTaskInstanceRun: () =>
      apiFetch<TaskDQRunRecord>(
        `/v1/dags/${encodeURIComponent(dagId)}/tasks/${encodeURIComponent(taskId)}` +
          `/runs/by_run/${encodeURIComponent(runId)}?map_index=${mapIndex}`,
      ),
  };
}
