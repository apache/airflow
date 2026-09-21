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

// Handlers for the `typescript_taskflow_example` Dag.
//
// `summarize` shows argument binding. Its Python `@task.stub` signature is snake_case and the
// interface below is camelCase, and neither side declares anything, because names bind by folding.
// Its `totals` argument takes `make_totals`'s output, so the handler never touches XCom for it.
//
// The Dag is served by the same bundle as `typescript_example`, so the pair of ids a handler binds
// is what tells its tasks apart.
// `buildSummaryMessage` implements a task named `build_message`, exactly as the other Dag has, and
// the two share nothing else.

import { getClient, getContext, withArgNames } from "apache-airflow-ts-sdk";

/** What `make_totals` returns on the Python side. */
export interface Totals {
  orders: number;
  revenue: number;
}

/**
 * Every argument the Dag's `summarize(...)` call binds.
 *
 * Python spells these `totals`, `region_code`, `currency`, `threshold` and
 * `dry_run`, and folding absorbs the difference. `totals` arrives as the
 * upstream task's value, not as a reference to it.
 */
export interface SummarizeArgs {
  totals: Totals;
  regionCode: string;
  currency: string;
  threshold: number;
  dryRun: boolean;
}

/** What {@link summarize} returns, and what {@link buildSummaryMessage} reads. */
export interface Summary {
  regionCode: string;
  orders: number;
  averageOrder: number;
  currency: string;
  passed: boolean;
  dryRun: boolean;
}

export async function summarize({
  totals,
  regionCode,
  currency,
  threshold,
  dryRun,
}: SummarizeArgs): Promise<Summary> {
  const average = totals.orders === 0 ? 0 : totals.revenue / totals.orders;
  const averageOrder = Number(average.toFixed(2));

  if (!dryRun) {
    await getClient().setXCom({
      key: "summary_line",
      value: `${regionCode}: ${totals.orders} orders`,
    });
  }

  return {
    regionCode,
    orders: totals.orders,
    averageOrder,
    currency,
    passed: averageOrder >= threshold,
    dryRun,
  };
}

/** Every argument the Dag's `report(...)` call binds, as the handler wants them. */
export interface ReportArgs {
  summary: Summary;
  /** The call's `run_label`, which folding cannot reach, so it is mapped below. */
  label: string;
}

/**
 * Renaming an argument the Python side named something else entirely.
 *
 * The mapping comes first, the handler second. `summary` is absent from the map
 * because folding already reaches it.
 */
export const report = withArgNames(
  { label: "run_label" },
  async ({ summary, label }: ReportArgs) => {
    if (label !== "nightly") {
      throw new Error(`expected run label "nightly" but got "${label}"`);
    }

    return {
      label,
      regionCode: summary.regionCode,
      healthy: summary.passed,
    };
  },
);

export async function buildSummaryMessage() {
  // Nothing was passed to this task, so its upstream's output is read explicitly.
  const ctx = getContext();
  const summary = await getClient().getXCom<Summary>({
    key: "return_value",
    taskId: "summarize",
  });
  if (summary === null) {
    throw new Error(`task ${ctx.taskId} has no summary to report`);
  }

  return {
    // The dag_id is in the return value on purpose: it is how the end-to-end test tells this task
    // apart from `typescript_example`'s `build_message`.
    dagId: ctx.dagId,
    message: `${summary.regionCode}: ${summary.orders} orders averaging ${summary.averageOrder} ${summary.currency}`,
  };
}
