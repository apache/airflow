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
import type { TaskInstanceState } from "openapi/requests/types.gen";

/** Part of the component's work has no live instance covering it; the rest does. */
export const DEGRADED = "degraded";

// The health endpoint reports its own vocabulary ("healthy" / "degraded" / "down" / "unhealthy"),
// so it is mapped onto the task-state palette the rest of the UI already colours badges with:
// "degraded" borrows the yellow of up_for_retry to distinguish partial coverage from none at all.
const HEALTH_STATES: Record<string, TaskInstanceState> = {
  [DEGRADED]: "up_for_retry",
  down: "failed",
  healthy: "success",
  unhealthy: "failed",
};

/** A null state paints the neutral palette, for a status this UI version has no mapping for. */
export const healthState = (status?: string | null): TaskInstanceState | null =>
  HEALTH_STATES[status ?? ""] ?? null;

export const healthTranslationKey = (status?: string | null) =>
  status !== null && status !== undefined && status in HEALTH_STATES
    ? `health.${status}`
    : "health.unknownStatus";
