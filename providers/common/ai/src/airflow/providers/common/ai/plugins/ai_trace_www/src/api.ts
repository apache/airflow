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

import type { AgentItem, ObservationIO, TraceListItem, TraceSummary } from "src/types";

function getBase(): string {
  if (typeof document === "undefined") return "/ai-trace";
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);
  const basePath = baseUrl.pathname.replace(/\/$/, "") || "";
  return basePath ? `${basePath}/ai-trace` : "/ai-trace";
}

const BASE = getBase();

export class ApiError extends Error {}

export async function fetchTraceSummary(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
): Promise<TraceSummary> {
  const qs =
    `dag_id=${encodeURIComponent(dagId)}` +
    `&run_id=${encodeURIComponent(runId)}` +
    `&task_id=${encodeURIComponent(taskId)}` +
    `&map_index=${mapIndex}`;
  const res = await fetch(`${BASE}/trace-summary?${qs}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = (body as { detail?: string }).detail;
    throw new ApiError(detail ?? res.statusText);
  }
  return res.json() as Promise<TraceSummary>;
}

export interface ListTracesParams {
  dagId?: string;
  minCost?: number;
  minLatency?: number;
  since?: string;
  state?: string;
  taskId?: string;
}

export async function fetchTraceList(params: ListTracesParams = {}): Promise<TraceListItem[]> {
  const usp = new URLSearchParams();
  if (params.dagId) usp.set("dag_id", params.dagId);
  if (params.taskId) usp.set("task_id", params.taskId);
  if (params.since) usp.set("since", params.since);
  if (params.state) usp.set("state", params.state);
  if (params.minLatency != null) usp.set("min_latency", String(params.minLatency));
  if (params.minCost != null) usp.set("min_cost", String(params.minCost));
  const qs = usp.toString();
  const res = await fetch(`${BASE}/traces${qs ? `?${qs}` : ""}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = (body as { detail?: string }).detail;
    throw new ApiError(detail ?? res.statusText);
  }
  const data = (await res.json()) as { items: TraceListItem[] };
  return data.items;
}

export async function fetchAgents(): Promise<AgentItem[]> {
  const res = await fetch(`${BASE}/agents`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = (body as { detail?: string }).detail;
    throw new ApiError(detail ?? res.statusText);
  }
  const data = (await res.json()) as { items: AgentItem[] };
  return data.items;
}

// Short-TTL cache: a finished task's trace is immutable, but a RUNNING agent's
// trace is still growing, so unlike observation IO this must expire.
const TRACE_TTL_MS = 60_000;
const traceCache = new Map<string, { at: number; promise: Promise<TraceSummary> }>();

export function fetchTraceById(traceId: string): Promise<TraceSummary> {
  const cached = traceCache.get(traceId);
  if (cached && Date.now() - cached.at < TRACE_TTL_MS) return cached.promise;
  const promise = (async () => {
    const res = await fetch(`${BASE}/trace/${encodeURIComponent(traceId)}`);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      const detail = (body as { detail?: string }).detail;
      throw new ApiError(detail ?? res.statusText);
    }
    return (await res.json()) as TraceSummary;
  })().catch((err: unknown) => {
    traceCache.delete(traceId);
    throw err;
  });
  traceCache.set(traceId, { at: Date.now(), promise });
  return promise;
}


// Observations are immutable once written, so cache their IO for the page's
// lifetime -- closing/reopening the modal must not re-hit the backend (which
// proxies to Langfuse). Poor man's react-query; failures are evicted so a
// transient error doesn't stick.
const obsIOCache = new Map<string, Promise<ObservationIO>>();

export function fetchObservationIO(obsId: string): Promise<ObservationIO> {
  const cached = obsIOCache.get(obsId);
  if (cached) return cached;
  const promise = (async () => {
    const res = await fetch(`${BASE}/observations/${encodeURIComponent(obsId)}`);
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      const detail = (body as { detail?: string }).detail;
      throw new ApiError(detail ?? res.statusText);
    }
    return (await res.json()) as ObservationIO;
  })().catch((err: unknown) => {
    obsIOCache.delete(obsId);
    throw err;
  });
  obsIOCache.set(obsId, promise);
  return promise;
}
