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
import type { FilterableSearchParamsKeys } from "src/utils/useFiltersHandler";

import { useFilterConfigs } from "./filterConfigs";
import { SearchParamsKeys } from "./searchParams";

/**
 * Query parameters owned by the page's persistent panel -- the grid, the gantt, the graph and the
 * task log -- rather than by whichever tab is open beside it. The details layout's NavTabs carries
 * all of them onto every tab link, so a filter added to one of these lists survives a tab change.
 *
 * A table's paging and sorting are deliberately absent. Those describe one tab's list, and carrying
 * them over would open the next tab's list on a page that list need not have.
 */

// The run filters on the grid's filter bar.
export const GRID_FILTER_KEYS: ReadonlyArray<FilterableSearchParamsKeys> = [
  SearchParamsKeys.RUN_ID_PATTERN,
  SearchParamsKeys.STATE,
  SearchParamsKeys.RUN_TYPE,
  SearchParamsKeys.TRIGGERING_USER_NAME_PATTERN,
  SearchParamsKeys.RUN_AFTER_RANGE,
];

// Joins the grid's filter bar while the gantt is showing.
export const GANTT_DATE_FILTER_KEYS: ReadonlyArray<FilterableSearchParamsKeys> = [
  SearchParamsKeys.START_DATE_RANGE,
  SearchParamsKeys.END_DATE_RANGE,
];

export const GRAPH_FILTER_KEYS: ReadonlyArray<SearchParamsKeys> = [
  SearchParamsKeys.GRAPH_OPERATOR,
  SearchParamsKeys.GRAPH_TASK_GROUP,
  SearchParamsKeys.GRAPH_TASK_STATE,
  SearchParamsKeys.GRAPH_MAP_INDEX,
  SearchParamsKeys.GRAPH_DURATION_GTE,
];

// The task stream filter has no SearchParamsKeys entries and writes these names straight to the url.
export const TASK_STREAM_SEARCH_PARAMS = {
  depth: "depth",
  downstream: "downstream",
  mode: "mode",
  root: "root",
  upstream: "upstream",
} as const;

// Written by the log header (level and source) and by the log search.
export const LOG_FILTER_KEYS: ReadonlyArray<SearchParamsKeys> = [
  SearchParamsKeys.LOG_LEVEL,
  SearchParamsKeys.SOURCE,
  SearchParamsKeys.LOG_SEARCH,
];

// A date range filter is listed by its filter key but writes a start and an end key, so resolve
// those through the same config the filter bar uses.
export const usePersistentSearchParamKeys = (): ReadonlyArray<string> => {
  const { getFilterConfig } = useFilterConfigs();

  const gridKeys = [...GRID_FILTER_KEYS, ...GANTT_DATE_FILTER_KEYS].flatMap((key) => {
    const { endKey, startKey } = getFilterConfig(key);

    return startKey !== undefined && endKey !== undefined ? [startKey, endKey] : [key];
  });

  return [...gridKeys, ...GRAPH_FILTER_KEYS, ...Object.values(TASK_STREAM_SEARCH_PARAMS), ...LOG_FILTER_KEYS];
};

/**
 * The query for a link that leaves one place on the page for another: `targetSearch` plus every
 * persistent key in `currentSearch`. A key the target sets itself (e.g. `try_number`) wins.
 */
export const mergePersistentSearchParams = (
  currentSearch: string,
  targetSearch: string | undefined,
  persistentKeys: ReadonlyArray<string>,
) => {
  const current = new URLSearchParams(currentSearch);
  const target = new URLSearchParams(targetSearch);

  for (const key of persistentKeys) {
    if (!target.has(key)) {
      for (const value of current.getAll(key)) {
        target.append(key, value);
      }
    }
  }

  return target.toString();
};
