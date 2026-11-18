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

// Task loops declared in TypeScript: a task group that runs again and again
// until a gate says stop. The TypeScript spelling of the Python example
// `example_task_loops.py`.
//
// `refine` is Newton's method for the square root of two. Each iteration
// improves the previous estimate and measures its error, and the loop stops
// once the error is small enough, or at ten iterations. `accumulate` only
// counts, so it needs no handler to decide.

import { Dag, getLoop, type LoopDecision, type XComSequence } from "@apache-airflow/ts-sdk";

type Metrics = { estimate: number; error: number };

export const dag = new Dag("typescript_loop_example", {
  schedule: "@daily",
  catchup: false,
  tags: ["typescript", "loop"],
  description: "Task loops declared in TypeScript: one decided by a handler, one by a count.",
  queue: "typescript",
});

const refine = dag.taskGroup("refine");

const improve = refine.task("improve", async () => {
  const loop = await getLoop();
  // The terminal task's result of the iteration before; null at the first.
  const estimate = loop.getPrevious<Metrics>()?.estimate ?? 1.0;
  return (estimate + 2.0 / estimate) / 2.0;
});

const evaluate = refine.task("evaluate", ({ estimate }: { estimate: number }) => ({
  estimate,
  error: Math.abs(estimate * estimate - 2),
}));

// The gate. It reads the terminal task's result as wired data.
async function accurateEnough({ result }: { result: Metrics }): Promise<LoopDecision> {
  const threshold = 0.000001;
  if (result.error < threshold) {
    return { stop: true, reason: `converged with error ${result.error.toPrecision(2)}` };
  }
  return { stop: false, reason: `error ${result.error.toPrecision(2)}, needs < ${threshold}` };
}

const report = dag.task("report", async ({ history }: { history: XComSequence<Metrics> }) => {
  const last = await history.at(-1);
  return {
    iterations: await history.length(),
    estimate: last.estimate,
    errors: (await history.slice()).map((metrics) => metrics.error),
  };
});

const finished = dag.task("finished", () => {
  console.log("Refinement finished.");
});

const estimate = improve();
const evaluated = evaluate({ estimate });
const refinement = refine.loop(accurateEnough, { result: evaluated }, { maxIterations: 10 });
// Outside the loop, the loop's all() reads every iteration's Metrics.
report({ history: refinement.all(evaluated) });
// Order only: finished runs once, after the last iteration.
refinement.before(finished());

const accumulate = dag.taskGroup("accumulate");

const increment = accumulate.task("increment", async () => {
  const loop = await getLoop();
  return (loop.getPrevious<number>() ?? 0) + 1;
});
increment();

accumulate.loop({ maxIterations: 3 }).after(refinement);
