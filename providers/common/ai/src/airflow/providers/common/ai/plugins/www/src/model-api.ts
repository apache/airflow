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

import { regionQuery, type TaskRegion } from "./region.ts";
import type { SkillInfo, UsageInfo } from "src/types/model";

function getApiBase(): string {
  if (typeof document === "undefined") return "/api/v2";
  const baseHref = document.querySelector("head > base")?.getAttribute("href") ?? "";
  const baseUrl = new URL(baseHref, globalThis.location.origin);
  const basePath = baseUrl.pathname.replace(/\/$/, "") || "";
  return basePath ? `${basePath}/api/v2` : "/api/v2";
}

const API_BASE = getApiBase();

// Namespaced to match airflow.providers.common.ai.utils.logging.MODEL_NAME_XCOM_KEY --
// avoids colliding with a user's own "model_name" XCom.
const MODEL_NAME_XCOM_KEY = "__AIRFLOW__COMMON_AI_MODEL_NAME__";
// Matches airflow.providers.common.ai.utils.logging.SKILLS_XCOM_KEY.
const SKILLS_XCOM_KEY = "__AIRFLOW__COMMON_AI_SKILLS__";

/** Read one XCom entry; returns `null` when the task hasn't pushed that key (404). */
async function fetchXComValue<T>(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
  key: string,
  region?: TaskRegion,
): Promise<T | null> {
  const path =
    `${API_BASE}/dags/${encodeURIComponent(dagId)}` +
    `/dagRuns/${encodeURIComponent(runId)}` +
    `/taskInstances/${encodeURIComponent(taskId)}` +
    `/xcomEntries/${encodeURIComponent(key)}`;
  // A task inside a task loop needs its pass's region, or the route rejects the read as ambiguous.
  const res = await fetch(`${path}?map_index=${mapIndex}&deserialize=true${regionQuery(region)}`, {
    credentials: "same-origin",
  });
  if (res.status === 404) return null;
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    const detail = (body as { detail?: unknown }).detail;
    // `statusText` is always empty over HTTP/2, and a 422's `detail` is an array of
    // validation errors (which would print as "[object Object]") rather than a string --
    // fall back to the status code so the error is never a blank or useless message.
    throw new Error(typeof detail === "string" && detail.length > 0 ? detail : `HTTP ${res.status}`);
  }
  const data = (await res.json()) as { value: T };
  return data.value;
}

/**
 * A value the XCom backend kept in external storage. The public XCom endpoint never runs the
 * backend's deserialize_value, so it returns the reference the backend stored in its place
 * (e.g. an ``s3://...`` path from common.io with a low xcom_objectstorage_threshold).
 */
export interface OffloadedValue {
  reference: string;
}

export function isOffloaded(value: unknown): value is OffloadedValue {
  return typeof value === "object" && value !== null && !Array.isArray(value) && "reference" in value;
}

export function createModelApi(
  dagId: string,
  runId: string,
  taskId: string,
  mapIndex: number,
  region?: TaskRegion,
) {
  return {
    fetchModelName: async (): Promise<string | OffloadedValue | null> => {
      const value = await fetchXComValue<string>(dagId, runId, taskId, mapIndex, MODEL_NAME_XCOM_KEY, region);
      // A reference is a string too. A model name rarely holds a URI scheme, but can: vLLM serves
      // ``--model s3://...`` as the name, which this reports as a reference.
      return typeof value === "string" && value.includes("://") ? { reference: value } : value;
    },
    fetchUsage: async (): Promise<UsageInfo | OffloadedValue | null> => {
      const value = await fetchXComValue<unknown>(dagId, runId, taskId, mapIndex, "usage", region);
      if (typeof value === "string") return { reference: value };
      return typeof value === "object" && value !== null ? (value as UsageInfo) : null;
    },
    fetchSkills: async (): Promise<SkillInfo[] | OffloadedValue | null> => {
      const value = await fetchXComValue<unknown>(dagId, runId, taskId, mapIndex, SKILLS_XCOM_KEY, region);
      if (typeof value === "string") return { reference: value };
      return Array.isArray(value) ? (value as SkillInfo[]) : null;
    },
  };
}
