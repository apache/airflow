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
import { useEffect, useRef } from "react";

import { useLocation, useSearchParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type { FilterConfig } from "src/components/FilterBar/types";

import { lastFiltersKey, presetFiltersDefaultKey } from "src/constants/localStorage";
import { SearchParamsKeys } from "src/constants/searchParams";

// Every URL param the filter pills own. Composite filters enumerate the params they manage
// (mapped to undefined to clear them), so their keys are present whatever the current value is.
const getFilterParamNames = (configs: Array<FilterConfig>): Array<string> =>
  configs.flatMap((config) => {
    if (config.toSearchParams !== undefined) {
      return Object.keys(config.toSearchParams(undefined));
    }

    if (config.type === "daterange") {
      return [config.key, config.startKey, config.endKey].filter(
        (param): param is string => param !== undefined,
      );
    }

    return config.matchModeKey === undefined ? [config.key] : [config.key, config.matchModeKey];
  });

const filterQuery = (search: string, names: Array<string>): string => {
  const source = new URLSearchParams(search);
  const query = new URLSearchParams();

  names.forEach((name) => {
    source.getAll(name).forEach((value) => {
      query.append(name, value);
    });
  });

  return query.toString();
};

/**
 * Remembers the filters the user is working with and restores them when the list is landed on
 * without a query, so returning from another page resumes the same view. An explicit query wins,
 * and so does a pinned default preset, which states where the list should always start.
 */
export const useRememberedFilters = (configs: Array<FilterConfig>) => {
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [lastFilters, setLastFilters] = useLocalStorage<string>(lastFiltersKey(pathname), "");
  const [defaultPresetName] = useLocalStorage<string | null>(presetFiltersDefaultKey(pathname), null);
  // The previous render's filters, or undefined before the landing render has been observed.
  const previousQuery = useRef<string | undefined>(undefined);
  const filterParams = getFilterParamNames(configs);
  const filterParamsKey = filterParams.join(",");

  useEffect(() => {
    const query = filterQuery(searchParams.toString(), filterParams);
    const observed = previousQuery.current;

    previousQuery.current = query;

    // The landing render is not a filter change, so landing without filters must not erase what
    // is remembered before the restore below has applied it.
    if (observed === query || (observed === undefined && query === "")) {
      return;
    }

    setLastFilters(query);
    // filterParams is rebuilt on every render; filterParamsKey captures when its contents change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterParamsKey, searchParams, setLastFilters]);

  // Restore once per mount. DagsFilters calls this after usePausedDefault, so a bare landing
  // resumes the remembered view rather than keeping only the paused seed.
  useEffect(() => {
    const params = new URLSearchParams(searchParams);

    params.delete(SearchParamsKeys.OFFSET);
    params.delete(SearchParamsKeys.CURSOR);

    if ([...params].length > 0 || defaultPresetName !== null) {
      return;
    }

    const remembered = filterQuery(lastFilters, getFilterParamNames(configs));

    if (remembered === "") {
      return;
    }

    // Replace rather than push: the restored query should not add a history entry that lands on
    // the bare page and restores itself again.
    setSearchParams(new URLSearchParams(remembered), { replace: true });
    // Mount-only on purpose: later edits and clears must not re-trigger the restore.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [pathname]);
};
