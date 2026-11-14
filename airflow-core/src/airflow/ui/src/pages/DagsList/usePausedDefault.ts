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
import { useEffect, useState } from "react";

import { useSearchParams } from "react-router-dom";

import {
  pausedStateToSearchParams,
  type PausedStateValue,
} from "src/components/FilterBar/filters/pausedStateParams";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";

const HIDE_PAUSED_DEFAULT: PausedStateValue = "not_paused";

const hasPausedFilter = (params: URLSearchParams) =>
  params.has(SearchParamsKeys.PAUSED) || params.has(SearchParamsKeys.SCHEDULING_STATE);

/**
 * Writes the ``hide_paused_dags_by_default`` default into the URL so it shows up as a
 * pill instead of filtering invisibly.
 *
 * Seeds once per page load. Removing the pill is how you ask to see paused Dags, so re-seeding
 * whenever the param goes missing would put it straight back and leave them unreachable.
 *
 * Returns the default paused state until it is in the URL, so callers can query with it from the first render.
 * ``hasSeeded`` flips only once the URL carries a filter: flipping it when the write is issued would
 * leave a render with neither the param nor the pending flag, and fire an unfiltered request.
 */
export const usePausedDefault = () => {
  const hidePausedDagsByDefault = Boolean(useConfig("hide_paused_dags_by_default"));
  const [searchParams, setSearchParams] = useSearchParams();
  const [hasSeeded, setHasSeeded] = useState(false);

  useEffect(() => {
    if (hasSeeded || !hidePausedDagsByDefault) {
      return;
    }

    if (hasPausedFilter(searchParams)) {
      setHasSeeded(true);

      return;
    }

    // Functional form is required: PresetFiltersMenu restores a default preset from a
    // deeper effect that commits first, so an object write would clobber it with stale params.
    setSearchParams(
      (prev) => {
        if (hasPausedFilter(prev)) {
          return prev;
        }

        const next = new URLSearchParams(prev);

        Object.entries(pausedStateToSearchParams(HIDE_PAUSED_DEFAULT)).forEach(([key, value]) => {
          if (value !== undefined) {
            next.set(key, value);
          }
        });

        return next;
      },
      { replace: true },
    );
  }, [hasSeeded, hidePausedDagsByDefault, searchParams, setSearchParams]);

  return hidePausedDagsByDefault && !hasSeeded && !hasPausedFilter(searchParams)
    ? HIDE_PAUSED_DEFAULT
    : undefined;
};
