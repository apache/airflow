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

/**
 * Color mapping for calendar states using theme colors
 */
export const CALENDAR_STATE_COLORS = {
  empty: "#ebedf0", // gray.200
  failed: {
    medium: "#EF4444", // red.500 - mostly failed
    pure: "#DC2626", // red.600
  },
  mixed: {
    moderate: "#EAB308", // yellow.500 - 40-60%
    poor: "#F97316", // orange.500 - 20-40%
  },
  other: "#9CA3AF", // gray.400
  planned: {
    pure: "#F1E7DA", // scheduled.200 (lighter tan)
  },
  queued: {
    pure: "#808080", // queued.600 (gray)
  },
  running: {
    pure: "#3182CE", // blue.600
  },
  success: {
    high: "#16A34A", // success.700 - ≥80%
    medium: "#22C55E", // success.500 - ≥60%
    pure: "#008000", // success.600
  },
} as const;

/**
 * Success rate thresholds for color mapping
 */
export const SUCCESS_RATE_THRESHOLDS = {
  HIGH: 0.8,
  MEDIUM: 0.6,
  MODERATE: 0.4,
  POOR: 0.2,
} as const;

/**
 * Supported DAG run states for calendar display
 */
export const SUPPORTED_DAG_RUN_STATES: Array<DagRunState> = [
  "success",
  "failed",
  "queued",
  "running",
  "planned",
];
