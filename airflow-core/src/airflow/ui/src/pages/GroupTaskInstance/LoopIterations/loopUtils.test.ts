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
import i18next from "i18next";
import { beforeAll, describe, expect, it } from "vitest";

import type { LoopSummaryResponse } from "openapi/requests/types.gen";

import commonLocale from "../../../../public/i18n/locales/en/common.json";
import dagLocale from "../../../../public/i18n/locales/en/dag.json";
import { reasonSentence } from "./loopUtils";

const i18n = i18next.createInstance();

const translate = (key: string, options?: Record<string, unknown>) => i18n.t(`dag:${key}`, options);

const buildSummary = (overrides: Partial<LoopSummaryResponse>): LoopSummaryResponse =>
  ({
    exit_criteria_name: "converged",
    failed_at_iteration: null,
    iterations: [],
    iterations_ran: 2,
    max_iterations: 5,
    reason: null,
    reason_task_id: "body.task",
    status: "running",
    stopped_at_iteration: 1,
    ...overrides,
  }) as unknown as LoopSummaryResponse;

describe("loop locale keys", () => {
  beforeAll(async () => {
    await i18n.init({
      defaultNS: "dag",
      lng: "en",
      ns: ["dag", "common"],
      resources: { en: { common: commonLocale, dag: dagLocale } },
    });
  });

  it.each([
    { reason: "cap_reached" },
    { reason: "criteria_met" },
    { exit_criteria_name: null, reason: "criteria_met" },
    { reason: "iteration_failed" },
    { reason: "not_converged" },
    { exit_criteria_name: null, reason: "not_converged" },
    { status: "running" },
    { status: "stopped_early" },
    { status: "ran_to_cap" },
    { status: "failed" },
    { status: "skipped" },
    { status: "removed" },
  ] as Array<Partial<LoopSummaryResponse>>)("resolves an outcome sentence for %o", (overrides) => {
    const sentence = reasonSentence(translate, buildSummary(overrides));

    expect(sentence).not.toMatch(/^loop\./u);
    expect(sentence).not.toContain("{{");
  });

  it.each(["notRun", "failed", "stop", "continue"])("resolves the %s decision label", (name) => {
    expect(i18n.exists(`dag:loop.decision.${name}`)).toBe(true);
  });

  it.each([
    "dag:loop.outcome",
    "dag:loop.iterations",
    "dag:loop.iteration",
    "dag:loop.target",
    "dag:loop.reached",
    "dag:loop.rules.behaviour",
    "dag:loop.rules.boundedFor",
    "dag:loop.rules.whileUntil",
    "dag:loop.rules.maxIterations",
    "dag:loop.rules.exitCriteria",
    "dag:loop.filter.all",
    "dag:loop.filter.invocation",
    "dag:loop.filter.label",
    "dag:loop.history.title",
    "dag:loop.history.iterationsAxis",
    "dag:loop.history.capLabel",
    "dag:loop.history.stats.medianIterations",
    "dag:loop.history.stats.converged",
    "dag:loop.history.stats.capHits",
    "common:taskInstance.loopIterations",
  ])("defines %s in the English bundle", (key) => {
    expect(i18n.exists(key)).toBe(true);
  });
});
