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

// Native TypeScript Dags that wait for a person: one Dag per human-in-the-loop feature.
//
// Pack them into one bundle and put it in a Dag bundle the Node coordinator parses:
//
//     airflow-ts-pack src/hitl.ts --outfile dist/hitl.min.mjs
//
// A human-input task's first run creates a request and parks the task in `awaiting_input`; its
// Node process exits, so no worker is held while it waits. Answer the request on the Required
// Actions page in the Airflow UI (or through the REST API), and the task resumes in a new process
// with the answer.

import { approval, Bundle, Dag, humanInput } from "apache-airflow-ts-sdk";
import type { HumanInputResult } from "apache-airflow-ts-sdk";

/** The queue the deployment's `[sdk] queue_to_coordinator` routes to the Node coordinator. */
const QUEUE = "typescript";

interface Report {
  version: string;
  changes: string[];
}

// 1. Approve or reject a release.
//
// The request's text is built from the upstream report, so the reviewer sees what they approve.
// "Approve": `publish` runs. "Reject": `sign_off` still succeeds, and `publish` is skipped.
const release = new Dag("ts_hitl_release", { queue: QUEUE, tags: ["typescript", "hitl"] });
const report = release.task(
  "build_report",
  async (): Promise<Report> => ({ version: "1.4.0", changes: ["Faster parsing", "New UI"] }),
)();
const signOff = release.task(
  "sign_off",
  approval({
    subject: ({ report }: { report: Report }) => `Ship release ${report.version}?`,
    // Markdown, shown below the subject.
    body: ({ report }: { report: Report }) =>
      ["Changes in this release:", ...report.changes.map((change) => `- ${change}`)].join("\n"),
  }),
)({ report });
signOff.before(release.task("publish", async () => "published")());

// 2. Choose one or more options.
//
// A generic choice never skips or fails anything: the next task receives the answer and decides.
const regions = new Dag("ts_hitl_regions", { queue: QUEUE, tags: ["typescript", "hitl"] });
const picked = regions.task(
  "choose_regions",
  humanInput({
    subject: "Which regions should the rollout reach?",
    options: ["us", "eu", "apac"],
    multiple: true,
  }),
)();
regions.task("deploy", async ({ choice }: { choice: HumanInputResult }) => ({
  deployedTo: choice.chosen_options,
  // `null` when no one answered and the defaults were applied.
  by: choice.responded_by_user?.name ?? "the timeout default",
}))({ choice: picked });

// 3. Fall back to a default when no one answers in time.
//
// After 30 seconds without an answer, "Approve" is chosen for the reviewer and `load` runs.
// Without `defaults`, the task would fail instead, and its retries would apply.
const nightly = new Dag("ts_hitl_timeout", { queue: QUEUE, tags: ["typescript", "hitl"] });
const autoApproved = nightly.task(
  "auto_approve",
  approval({ subject: "Approve the nightly load?", defaults: "Approve", responseTimeout: 30 }),
)();
autoApproved.before(nightly.task("load", async () => "loaded")());

// 4. Fail the run on "Reject".
//
// With `onReject: "fail"`, a rejection fails `gate`, so the run shows as failed and `after_gate`
// does not run. The default, `"skip"`, would succeed and skip `after_gate` instead.
const strict = new Dag("ts_hitl_strict", { queue: QUEUE, tags: ["typescript", "hitl"] });
const gate = strict.task("gate", approval({ subject: "Proceed?", onReject: "fail" }))();
gate.before(strict.task("after_gate", async () => "ran")());

// 5. Let only one user answer.
//
// `id` is the user's id in the deployment's auth manager: replace "op" with one of your users.
// Anyone else who opens the request is refused when they answer.
const assigned = new Dag("ts_hitl_assigned", { queue: QUEUE, tags: ["typescript", "hitl"] });
assigned.task(
  "assigned_review",
  approval({ subject: "Approve the payroll export?", assignees: [{ id: "op", name: "Operator" }] }),
)();

await new Bundle(release, regions, nightly, strict, assigned).serve();
