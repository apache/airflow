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

import type { TraceListItem } from "src/types";

export type TimeWindow = "1h" | "24h" | "7d" | "all";

export function sinceFromWindow(w: TimeWindow): string | undefined {
  if (w === "all") return undefined;
  const ms = w === "1h" ? 3_600_000 : w === "24h" ? 86_400_000 : 7 * 86_400_000;
  return new Date(Date.now() - ms).toISOString();
}

export function percentile(values: number[], p: number): number | null {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  // Linear interpolation between closest ranks -- the naive floor-index
  // approach reports the MAX as p50 at N=2 (caught by review: two traces of
  // 94ms and 32.4s both showed "32.4s p50").
  const rank = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(rank);
  const hi = Math.ceil(rank);
  const loVal = sorted[lo] ?? null;
  const hiVal = sorted[hi] ?? null;
  if (loVal === null || hiVal === null) return loVal ?? hiVal;
  return loVal + (hiVal - loVal) * (rank - lo);
}

export interface Aggregates {
  count: number;
  p50Latency: number | null;
  p95Latency: number | null;
  totalCost: number;
}

export function aggregate(items: TraceListItem[]): Aggregates {
  const latencies = items.map((t) => t.latency).filter((v): v is number => typeof v === "number");
  const totalCost = items.reduce((sum, t) => sum + (typeof t.cost === "number" ? t.cost : 0), 0);
  return {
    count: items.length,
    p50Latency: percentile(latencies, 50),
    p95Latency: percentile(latencies, 95),
    totalCost,
  };
}

export function fmtTimestamp(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString();
}

export function fmtLatency(s: number | null | undefined): string {
  if (s == null) return "—";
  if (s < 1) return `${Math.round(s * 1000)}ms`;
  return `${s.toFixed(1)}s`;
}

export function fmtCost(c: number | null | undefined): string {
  if (c == null) return "—";
  if (c === 0) return "$0.0000";
  if (c < 0.01) return "<$0.01";
  return `$${c.toFixed(4)}`;
}
