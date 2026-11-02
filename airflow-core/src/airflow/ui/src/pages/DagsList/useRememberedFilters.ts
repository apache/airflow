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

import type { FilterConfig, FilterValue } from "src/components/FilterBar/types";

import { lastFiltersKey, presetFiltersDefaultKey } from "src/constants/localStorage";
import { getFilterParamNames } from "src/utils/useFiltersHandler";

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
 * Remembers the filters the user is working with and restores them whenever the list is landed on
 * without a query, so returning from another page resumes the same view. Clearing the filters
 * through the filter bar is how the list forgets them. An explicit query wins, and so does a
 * pinned default preset, which states where the list should always start.
 *
 * Returns the filter change handler to hand to ``FilterBar``, wrapped so a clear is told apart from
 * navigation to the bare list.
 */
export const useRememberedFilters = (
  configs: Array<FilterConfig>,
  handleFiltersChange: (filters: Record<string, FilterValue>) => void,
) => {
  const { pathname } = useLocation();
  const [searchParams, setSearchParams] = useSearchParams();
  const [lastFilters, setLastFilters] = useLocalStorage<string>(lastFiltersKey(pathname), "");
  const [defaultPresetName] = useLocalStorage<string | null>(presetFiltersDefaultKey(pathname), null);
  // The filters the previous render saw, or undefined before the first one has been observed.
  const previousQuery = useRef<string | undefined>(undefined);
  // Whether the URL change being observed came from the filter bar rather than from navigation.
  const edited = useRef(false);
  const filterParams = configs.flatMap((config) => getFilterParamNames(config));
  const filterParamsKey = filterParams.join(",");

  const onFiltersChange = (filters: Record<string, FilterValue>) => {
    edited.current = true;
    handleFiltersChange(filters);
  };

  useEffect(() => {
    const query = filterQuery(searchParams.toString(), filterParams);
    const observed = previousQuery.current;

    previousQuery.current = query;

    if (query === "") {
      const userEdited = edited.current;

      edited.current = false;

      if (userEdited) {
        setLastFilters("");
      } else if (defaultPresetName === null) {
        const remembered = filterQuery(lastFilters, filterParams);

        if (remembered !== "") {
          previousQuery.current = remembered;

          // Replace rather than push: the restored query should not add a history entry that lands
          // on the bare page and restores itself again.
          setSearchParams(new URLSearchParams(remembered), { replace: true });
        }
      }

      return;
    }

    edited.current = false;

    if (observed !== query) {
      setLastFilters(query);
    }
    // filterParams is rebuilt on every render; filterParamsKey captures when its contents change.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterParamsKey, searchParams, setLastFilters]);

  return onFiltersChange;
};
