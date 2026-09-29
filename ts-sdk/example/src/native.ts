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

// A Dag declared entirely in TypeScript: no Python file declares its graph.
//
// The counterpart of `main.ts`, which supplies handlers for a Dag that a Python
// file declares. Here the schedule, the tasks, their options and the edges
// between them are all written on this side, and the bundle answers the Dag
// processor's parse request with the serialized Dag.
//
// The graph is a graph rather than a chain, so the e2e suites exercise the
// constructs against each other: a task group, a named fan-in, order-only
// edges, a conditional, a multi-way branch, and a task that triggers another
// Dag's run.
//
// `main.ts` registers this Dag on the same bundle as the mixed-language
// handlers, so one artifact covers both authoring modes.

import { Dag, getClient } from "apache-airflow-ts-sdk";

/** The regions this example works over. */
const REGIONS = ["north", "south"] as const;

interface RegionRows {
  readonly region: string;
  readonly rows: number;
}

export const dag = new Dag("typescript_native_example", {
  schedule: "@daily",
  catchup: false,
  tags: ["typescript", "native"],
  description: "A Dag whose graph, schedule and task options are all declared in TypeScript.",
  // Routes every task of this Dag to the Node coordinator; see the
  // `queue_to_coordinator` entry in the TypeScript SDK docs.
  queue: "typescript",
});

// --- Extraction, inside a task group -------------------------------------
//
// A group prefixes the ids of everything in it, so these become
// `extract.north` and `extract.south`.

const extract = dag.taskGroup("extract");

const extractNorth = extract.task("north", async (): Promise<RegionRows> => {
  const configured = await getClient().getVariable("typescript_native_north_rows");
  return { region: "north", rows: Number(configured ?? 3) };
});

const extractSouth = extract.task("south", async (): Promise<RegionRows> => {
  const configured = await getClient().getVariable("typescript_native_south_rows");
  return { region: "south", rows: Number(configured ?? 0) };
});

// --- A named fan-in -------------------------------------------------------
//
// Two upstreams feed one task under the argument names its handler declares,
// which is what the wiring object is for.

const summarize = dag.task(
  "summarize",
  async ({ north, south }: { north: RegionRows; south: RegionRows }) => {
    const total = north.rows + south.rows;
    await getClient().setXCom({ key: "region_total", value: total });
    return { total, regions: REGIONS.length };
  },
);

const summarized = summarize({ north: extractNorth(), south: extractSouth() });

// --- A conditional --------------------------------------------------------
//
// The guarded tasks take no parameter for the control edge: the condition's
// boolean is a run-time signal rather than data, so it is never an argument.

const loadRows = dag.task("load_rows", async () => {
  const total = await getClient().getXCom<number>({ key: "region_total", taskId: "summarize" });
  return { loaded: total ?? 0 };
});

const reportEmpty = dag.task("report_empty", async () => ({ loaded: 0 }));

const loaded = loadRows();
const reportedEmpty = reportEmpty();

// The upstream is an input like any other, named after the argument it
// supplies.
const hasRows = dag.task(
  "has_rows",
  async ({ summary }: { summary: { total: number } }) => summary.total > 0,
);
const gated = hasRows({ summary: summarized });

dag.if(gated).then(loaded).else(reportedEmpty);

// --- A multi-way branch ---------------------------------------------------
//
// A case is the reference the SDK handed back, so renaming a handler cannot
// silently rewire the Dag, and the compiler checks the candidate exists.

const publishDaily = dag.task("publish_daily", async () => ({ cadence: "daily" }));
const publishWeekly = dag.task("publish_weekly", async () => ({ cadence: "weekly" }));

const daily = publishDaily();
const weekly = publishWeekly();

// Decided from a Variable rather than from an upstream value, so the decider
// takes no argument and the extract group is what orders it.
const pickCadence = dag.task("pick_cadence", async () => {
  const cadence = await getClient().getVariable("typescript_native_cadence");
  return cadence === "weekly" ? weekly : daily;
});
const picked = pickCadence();

dag.switch(picked).case(daily).case(weekly);

// --- Order-only edges -----------------------------------------------------
//
// `cleanup` follows whichever side of each branch ran, but takes nothing from
// it, so the edges are drawn between the references rather than through an
// argument. Its `all_done` rule is what lets it run when a branch skipped the
// other side.
//
// `extract.before(picked)` is the same kind of edge with a group at one end:
// the decider reads no upstream value, so this is what puts it after every
// task the group holds.

const cleanup = dag.task("cleanup", async () => undefined, { triggerRule: "all_done" });
const cleaned = cleanup();

cleaned.after(loaded, reportedEmpty, daily, weekly);
extract.before(picked);

// --- A task Python runs ---------------------------------------------------
//
// A trigger wraps no TypeScript function: it serializes as
// `TriggerDagRunOperator` and a Python worker executes it, so it inherits no
// queue from this Dag and needs nothing to call.

dag
  .triggerDagRun({
    taskId: "trigger_downstream",
    dagId: "typescript_example",
    conf: { triggered_by: "{{ dag.dag_id }}" },
  })
  .after(cleaned);
