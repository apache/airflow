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

import type { SessionResponse } from "src/types/feedback";

function getBase(): string {
  if (typeof document === "undefined") return "/hitl-review";
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);
  const basePath = baseUrl.pathname.replace(/\/$/, "") || "";
  return basePath ? `${basePath}/hitl-review` : "/hitl-review";
}

const BASE = getBase();

function buildQs(dagId: string, runId: string, taskId: string, mapIndex: number): string {
  return (
    `dag_id=${encodeURIComponent(dagId)}` +
    `&run_id=${encodeURIComponent(runId)}` +
    `&task_id=${encodeURIComponent(taskId)}` +
    `&map_index=${mapIndex}`
  );
}

export class ApiError extends Error {
  taskActive?: boolean;

  constructor(message: string, taskActive?: boolean) {
    super(message);
    this.taskActive = taskActive;
  }
}

async function apiFetch<T>(
  path: string,
  qs: string,
  init?: RequestInit,
): Promise<T> {
  const sep = path.includes("?") ? "&" : "?";
  const res = await fetch(`${BASE}${path}${sep}${qs}`, init);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = (body as { detail?: string | { message?: string; task_active?: boolean } }).detail;
    let message: string;
    let taskActive: boolean | undefined;
    if (typeof detail === "object" && detail !== null) {
      message = detail.message ?? res.statusText;
      taskActive = detail.task_active;
    } else {
      message = detail ?? res.statusText;
    }
    throw new ApiError(message, taskActive);
  }
  return res.json() as Promise<T>;
}

export function createApi(dagId: string, runId: string, taskId: string, mapIndex: number) {
  const qs = buildQs(dagId, runId, taskId, mapIndex);

  return {
    fetchSession: () =>
      apiFetch<SessionResponse>("/sessions/find", qs),

    submitFeedback: (feedback: string) =>
      apiFetch<SessionResponse>("/sessions/feedback", qs, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ feedback }),
      }),

    approve: () =>
      apiFetch<SessionResponse>("/sessions/approve", qs, { method: "POST" }),

    reject: () =>
      apiFetch<SessionResponse>("/sessions/reject", qs, { method: "POST" }),
  };
}
