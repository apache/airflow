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

/**
 * The Paused state pill covers four choices backed by the two URL params that already exist:
 *
 *   "not_paused" → ``paused=false``              unpaused, which includes draining Dags
 *   "active"     → ``scheduling_state=active``    excludes paused and draining Dags
 *   "paused"     → ``paused=true``
 *   "draining"   → ``scheduling_state=draining``
 *
 * Draining is a subset of not-paused rather than a sibling of it — a draining Dag has not been
 * paused yet, it just stopped taking new runs — so it needs the narrower param. Keeping
 * ``paused`` for the other two leaves existing links and the ``hide_paused_dags_by_default``
 * seed working untouched.
 */
export type PausedStateValue = "active" | "draining" | "not_paused" | "paused";

export const pausedStateToSearchParams = (value: FilterValue): Record<string, string | undefined> => {
  const paused = value === "not_paused" ? "false" : undefined;

  return {
    [SearchParamsKeys.PAUSED]: value === "paused" ? "true" : paused,
    [SearchParamsKeys.SCHEDULING_STATE]: value === "draining" || value === "active" ? value : undefined,
  };
};

export const pausedStateFromSearchParams = (params: URLSearchParams): PausedStateValue | undefined => {
  const schedulingState = params.get(SearchParamsKeys.SCHEDULING_STATE);

  // Scheduling state is narrower than paused=false, which also includes draining Dags.
  if (schedulingState === "draining" || schedulingState === "active") {
    return schedulingState;
  }

  // ``scheduling_state=paused`` is the same set as ``paused=true``, so links written against the
  // short-lived Scheduling state filter keep working.
  if (schedulingState === "paused") {
    return "paused";
  }

  const paused = params.get(SearchParamsKeys.PAUSED);

  if (paused === "true") {
    return "paused";
  }

  return paused === "false" ? "not_paused" : undefined;
};

/** The query params to send for a pill value, so the list can never filter on more than the pill shows. */
export const pausedStateToQuery = (value: PausedStateValue | undefined) => {
  let paused;

  if (value === "paused") {
    paused = true;
  } else if (value === "not_paused") {
    paused = false;
  }

  return { paused, schedulingState: value === "draining" || value === "active" ? value : undefined };
};
