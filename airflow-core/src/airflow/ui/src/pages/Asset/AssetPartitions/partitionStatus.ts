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
import type { AssetPartitionResponse, TaskInstanceState } from "openapi/requests/types.gen";

export type PartitionStatus = AssetPartitionResponse["status"];

export const PARTITION_STATUSES: Array<PartitionStatus> = ["materialized", "in_progress", "failed"];

// A partition has no state of its own: it borrows the colour and icon of the run state it stands for.
export const RUN_STATE_BY_STATUS: Record<PartitionStatus, TaskInstanceState> = {
  failed: "failed",
  in_progress: "running",
  materialized: "success",
};

export const STATUS_LABEL_KEY: Record<PartitionStatus, string> = {
  failed: "common:states.failed",
  in_progress: "assets:partitions.status.in_progress",
  materialized: "assets:partitions.status.materialized",
};

// The default cron partition key of a daily Dag is the day at midnight.
const DAY_KEY = /^(?<month>\d{4}-\d{2})-(?<day>\d{2})(?:T00:00:00)?$/u;

export type MonthOfDays = {
  readonly days: Map<number, AssetPartitionResponse>;
  readonly month: string;
};

/**
 * Group partitions by month when every key is a calendar day (`YYYY-MM-DD`, with or without `T00:00:00`).
 * Keys are free-form strings, so any other key means there is no day grid to draw.
 */
export const groupByMonth = (partitions: Array<AssetPartitionResponse>): Array<MonthOfDays> | undefined => {
  const months = new Map<string, Map<number, AssetPartitionResponse>>();

  for (const partition of partitions) {
    const groups = DAY_KEY.exec(partition.partition_key)?.groups;
    const day = Number(groups?.day);

    const days = months.get(groups?.month ?? "") ?? new Map<number, AssetPartitionResponse>();

    // Two keys on one day, such as `2026-01-01` and `2026-01-01T00:00:00`, cannot share a cell.
    if (groups?.month === undefined || day < 1 || day > 31 || days.has(day)) {
      return undefined;
    }

    days.set(day, partition);
    months.set(groups.month, days);
  }

  return months.size === 0
    ? undefined
    : [...months.entries()]
        .sort(([first], [second]) => first.localeCompare(second))
        .map(([month, days]) => ({ days, month }));
};
