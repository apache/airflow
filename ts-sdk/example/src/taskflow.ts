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
// `summarize` shows argument binding: its Python `@task.stub` signature is
// snake_case, the interface below is camelCase, and nothing is declared on
// either side, because names bind by folding.
//
// The Dag is served by the same bundle as `typescript_example`, so the pair of
// ids a handler binds is what tells its tasks apart. `buildSummaryMessage`
// implements a task named `build_message`, exactly as the other Dag has, and
// the two share nothing else.

import { getClient, getContext } from "apache-airflow-ts-sdk";

/** What `make_totals` returns on the Python side. */
export interface Totals {
  orders: number;
  revenue: number;
}

/**
 * Every argument the Dag's `summarize(...)` call binds.
 *
 * Python spells these `region_code`, `currency`, `threshold` and `dry_run`.
 * Folding absorbs the difference, so this interface says what the handler
 * wants to read them as and nothing more.
 */
export interface SummarizeArgs {
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
  regionCode,
  currency,
  threshold,
  dryRun,
}: SummarizeArgs): Promise<Summary> {
  const client = getClient();
  // Read explicitly: an upstream's return value is not a bound argument unless
  // the Python call passes it, and this Dag's `summarize(...)` call does not.
  const totals = await client.getXCom<Totals>({
    key: "return_value",
    taskId: "make_totals",
  });
  if (totals === null) {
    throw new Error(`task ${getContext().taskId} has no totals to summarize`);
  }
  const average = totals.orders === 0 ? 0 : totals.revenue / totals.orders;
  const averageOrder = Number(average.toFixed(2));

  if (!dryRun) {
    await client.setXCom({
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

export async function buildSummaryMessage() {
  const ctx = getContext();
  const summary = await getClient().getXCom<Summary>({
    key: "return_value",
    taskId: "summarize",
  });
  if (summary === null) {
    throw new Error(`task ${ctx.taskId} has no summary to report`);
  }

  return {
    // The dag_id is in the return value on purpose: it is how the end-to-end
    // test tells this task apart from `typescript_example`'s `build_message`.
    dagId: ctx.dagId,
    message: `${summary.regionCode}: ${summary.orders} orders averaging ${summary.averageOrder} ${summary.currency}`,
  };
}
