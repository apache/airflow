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

/** Mirrors ``airflow.providers.common.dataquality.results.DQRun.to_dict()``. */
export interface DQRunRecord {
  asset_names: Array<string>;
  dag_id: string;
  finished_at: string | null;
  map_index: number;
  run_id: string;
  run_uid: string;
  ruleset_name: string | null;
  started_at: string | null;
  table_ref: string | null;
  task_id: string;
  try_number: number;
}

export type RuleStatus = "error" | "fail" | "pass" | "warn";

export type RuleSeverity = "error" | "warn";

/** Mirrors ``airflow.providers.common.dataquality.results.RuleResult.to_dict()``. */
export interface RuleResultRecord {
  condition: Record<string, number>;
  description?: string | null;
  dimension: string;
  duration_ms: number | null;
  error_message: string | null;
  observed_value: number | string | null;
  rule_name: string;
  rule_uid: string;
  severity: RuleSeverity;
  sql?: string | null;
  status: RuleStatus;
}

export interface DQSummaryRecord {
  errored: number;
  failed: number;
  passed: number;
  score: number | null;
  warned: number;
}

export interface TaskDQRunRecord {
  results: Array<RuleResultRecord>;
  run: DQRunRecord;
  summary: DQSummaryRecord;
}

export interface RuleHistoryRecord extends RuleResultRecord {
  run: {
    dag_id: string;
    map_index?: number;
    run_id: string;
    run_uid: string;
    started_at: string | null;
    table_ref: string | null;
    task_id: string;
  };
}

/** Mirrors the ``{"items": [...], "next_cursor": ...}`` shape returned by paginated routes. */
export interface PaginatedResult<T> {
  items: Array<T>;
  next_cursor: string | null;
}
