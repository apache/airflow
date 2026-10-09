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
import { SearchParamsKeys } from "src/constants/searchParams";

import type { FilterValue } from "../types";

// Sentinel for "every iteration of this loop". It is not written to the URL -- the absence of
// an iteration param is what means "all" -- but the select needs a value to render as chosen.
export const ITERATION_ALL = "all";

export type LoopFilterValue = {
  // Absent means every iteration; otherwise the index as it appears in the loop summary.
  iteration?: string;
  loopId: string;
};

export const isLoopFilterValue = (value: FilterValue): value is LoopFilterValue =>
  typeof value === "object" && value !== null && !Array.isArray(value) && "loopId" in value;

// One pill over the two params the task instance endpoint takes. A loop is always part of the
// value, so an iteration can never be set on its own -- which the server answers with nothing,
// since an iteration index means nothing until you say which loop it counts.
export const loopToSearchParams = (value: FilterValue): Record<string, string | undefined> => {
  const selection = isLoopFilterValue(value) ? value : undefined;

  return {
    [SearchParamsKeys.ITERATION]: selection?.iteration,
    [SearchParamsKeys.LOOP_ID]: selection?.loopId,
  };
};

export const loopFromSearchParams = (params: URLSearchParams): LoopFilterValue | undefined => {
  const loopId = params.get(SearchParamsKeys.LOOP_ID);

  // An iteration without a loop is not a filter anyone can act on, so it is dropped rather
  // than shown as a pill that matches nothing.
  if (loopId === null || loopId === "") {
    return undefined;
  }

  const iteration = params.get(SearchParamsKeys.ITERATION);

  return { iteration: iteration === null || iteration === "" ? undefined : iteration, loopId };
};
