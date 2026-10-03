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
import type { LoopIterationSummary, LoopSummaryResponse } from "openapi/requests/types.gen";

/** Value of the iteration URL parameter that selects every iteration. */
export const ITERATION_ALL = "all";

const FAILED_STATES = new Set(["failed", "upstream_failed"]);

type ResultTag = {
  /** Absent when the result is a bare value rather than a set of named fields. */
  readonly label?: string;
  readonly value: string;
};

/** A value short and flat enough to read on the iteration's own line. Dates arrive as ISO strings. */
const isScalar = (value: unknown): value is boolean | number | string =>
  typeof value === "string" || typeof value === "number" || typeof value === "boolean";

export const buildResultTags = (value: unknown): { hasComplex: boolean; tags: Array<ResultTag> } => {
  if (value === null || value === undefined) {
    return { hasComplex: false, tags: [] };
  }
  if (isScalar(value)) {
    return { hasComplex: false, tags: [{ value: String(value) }] };
  }
  if (Array.isArray(value)) {
    return { hasComplex: true, tags: [] };
  }

  const entries = Object.entries(value as Record<string, unknown>);

  return {
    hasComplex: entries.some(([, entry]) => !isScalar(entry)),
    tags: entries
      .filter((entry): entry is [string, boolean | number | string] => isScalar(entry[1]))
      .map(([label, entry]) => ({ label, value: String(entry) })),
  };
};

const lastRanIteration = (summary: LoopSummaryResponse): LoopIterationSummary | undefined => {
  for (let index = summary.iterations.length - 1; index >= 0; index -= 1) {
    const iteration = summary.iterations[index];

    if (iteration !== undefined && !(iteration.is_tail ?? false)) {
      return iteration;
    }
  }

  return undefined;
};

/** The criteria's own numbers for that iteration, when the criteria was stated as data. */
export const finalCriteria = (summary: LoopSummaryResponse) =>
  lastRanIteration(summary)?.criteria ?? undefined;

export const reasonSentence = (
  translate: (key: string, options?: Record<string, unknown>) => string,
  summary: LoopSummaryResponse,
): string => {
  const comparison = finalCriteria(summary);
  const criteria =
    comparison === undefined
      ? summary.exit_criteria_name
      : `${comparison.field} ${comparison.op} ${String(comparison.target)}`;
  const options = {
    criteria,
    index: summary.stopped_at_iteration ?? summary.failed_at_iteration ?? 0,
    max: summary.max_iterations,
    ran: summary.iterations_ran,
    taskId: summary.reason_task_id,
  };

  switch (summary.reason) {
    case "cap_reached":
      return translate("loop.reason.cap_reached", options);
    case "criteria_met":
      return translate(
        criteria === null || criteria === undefined
          ? "loop.reason.criteria_met_unnamed"
          : "loop.reason.criteria_met",
        options,
      );
    case "iteration_failed":
      return translate("loop.reason.iteration_failed", options);
    case "not_converged":
      return translate(
        criteria === null || criteria === undefined
          ? "loop.reason.not_converged_unnamed"
          : "loop.reason.not_converged",
        options,
      );
    case null:
    case undefined:
    default:
      return translate(`loop.reason.${summary.status}`, options);
  }
};

/** Per-iteration decision text: the tail never ran, a failure overrides the missing signal. */
export const decisionLabel = (
  translate: (key: string) => string,
  iteration: LoopIterationSummary,
): string | undefined => {
  if (iteration.is_tail ?? false) {
    return translate("loop.decision.notRun");
  }
  if (iteration.state !== null && iteration.state !== undefined && FAILED_STATES.has(iteration.state)) {
    return translate("loop.decision.failed");
  }
  if (iteration.decision === "stop") {
    return translate("loop.decision.stop");
  }

  return iteration.decision === "continue" ? translate("loop.decision.continue") : undefined;
};
