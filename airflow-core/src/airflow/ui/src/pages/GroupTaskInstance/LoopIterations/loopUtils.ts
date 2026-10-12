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
import type { LoopSummaryResponse } from "openapi/requests/types.gen";

/** Value of the iteration URL parameter that selects every iteration. */
export const ITERATION_ALL = "all";

/** The URL's iteration while the loop still has it, otherwise the newest iteration (or all when none ran). */
export const getSelectedIteration = (param: string | null, summary: LoopSummaryResponse): string => {
  if (
    param !== null &&
    (param === ITERATION_ALL || summary.iterations.some((iteration) => String(iteration.index) === param))
  ) {
    return param;
  }
  const latest = summary.iterations.at(-1)?.index;

  return latest === undefined ? ITERATION_ALL : String(latest);
};

export const reasonSentence = (
  translate: (key: string, options?: Record<string, unknown>) => string,
  summary: LoopSummaryResponse,
): string => {
  const options = {
    index: summary.stopped_at_iteration ?? summary.failed_at_iteration ?? 0,
    max: summary.max_iterations,
    ran: summary.iterations_ran,
    taskId: summary.reason_task_id,
  };

  switch (summary.reason) {
    case "cap_reached":
      return translate("loop.reason.cap_reached", options);
    case "iteration_failed":
      return translate("loop.reason.iteration_failed", options);
    case null:
    case undefined:
    default:
      return translate(`loop.reason.${summary.status}`, options);
  }
};
