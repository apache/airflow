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
import type { DagRunState } from "./types";

export const CALENDAR_STATE_COLORS = {
  empty: "#ebedf0",
  failed: {
    medium: "#EF4444",
    pure: "#DC2626",
  },
  mixed: {
    moderate: "#EAB308",
    poor: "#F97316",
  },
  other: "#9CA3AF",
  planned: {
    pure: "#F1E7DA",
  },
  queued: {
    pure: "#808080",
  },
  running: {
    pure: "#3182CE",
  },
  success: {
    high: "#16A34A",
    medium: "#22C55E",
    pure: "#008000",
  },
} as const;

export const SUCCESS_RATE_THRESHOLDS = {
  HIGH: 0.8,
  MEDIUM: 0.6,
  MODERATE: 0.4,
  POOR: 0.2,
} as const;

export const SUPPORTED_DAG_RUN_STATES: Array<DagRunState> = [
  "success",
  "failed",
  "queued",
  "running",
  "planned",
];
