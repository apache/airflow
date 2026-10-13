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
import { useSearchParams } from "react-router-dom";
import { useDebounce } from "use-debounce";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

import type { AggregationMode, DagRunLimit, TimeScale, ViewMode } from "./types";

type UseTimeScheduleFiltersProps = {
  readonly aggregationMode: AggregationMode;
  readonly dagRunLimit: DagRunLimit;
  readonly selectedTimezone: string;
  readonly timeScale: TimeScale;
  readonly viewMode: ViewMode;
};

export const useTimeScheduleFilters = ({
  aggregationMode,
  dagRunLimit,
  selectedTimezone,
  timeScale,
  viewMode,
}: UseTimeScheduleFiltersProps) => {
  const [searchParams] = useSearchParams({ [SearchParamsKeys.SHOW_SCHEDULED_ONLY]: "true" });
  const [streamTimeScale] = useDebounce(timeScale, 200);
  const multiTeamEnabled = Boolean(useConfig("multi_team"));
  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.DAG_ID_PATTERN,
    SearchParamsKeys.STATE,
    SearchParamsKeys.RUN_TYPE,
    SearchParamsKeys.RUN_AFTER_RANGE,
    SearchParamsKeys.START_DATE_RANGE,
    SearchParamsKeys.DURATION_GTE,
    SearchParamsKeys.DURATION_LTE,
    SearchParamsKeys.TAGS,
    SearchParamsKeys.TIMETABLE_TYPE,
    SearchParamsKeys.PAUSED,
    SearchParamsKeys.SHOW_SCHEDULED_ONLY,
  ];

  if (multiTeamEnabled) {
    searchParamKeys.push(SearchParamsKeys.TEAMS);
  }
  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);
  const buildStreamQuery = () => {
    const query = new URLSearchParams();
    const forwardedKeys = [
      SearchParamsKeys.DAG_ID_PATTERN,
      SearchParamsKeys.STATE,
      SearchParamsKeys.RUN_TYPE,
      SearchParamsKeys.RUN_AFTER_GTE,
      SearchParamsKeys.RUN_AFTER_LTE,
      SearchParamsKeys.START_DATE_GTE,
      SearchParamsKeys.START_DATE_LTE,
      SearchParamsKeys.DURATION_GTE,
      SearchParamsKeys.DURATION_LTE,
      SearchParamsKeys.TAGS,
      SearchParamsKeys.TAGS_MATCH_MODE,
      SearchParamsKeys.TIMETABLE_TYPE,
      SearchParamsKeys.PAUSED,
      ...(multiTeamEnabled ? [SearchParamsKeys.TEAMS] : []),
    ];

    forwardedKeys.forEach((key) =>
      searchParams.getAll(key).forEach((value) => {
        if (value !== "") {
          query.append(key, value);
        }
      }),
    );
    query.set("aggregation_mode", aggregationMode);
    query.set("limit", String(dagRunLimit));
    query.set(
      "show_scheduled_only",
      String(searchParams.get(SearchParamsKeys.SHOW_SCHEDULED_ONLY) === "true"),
    );
    query.set("time_scale", String(streamTimeScale));
    query.set("timezone", selectedTimezone);
    query.set("view_mode", viewMode);

    return query.toString();
  };
  const streamQuery = buildStreamQuery();

  return {
    controls: {
      filterConfigs,
      initialValues: {
        ...initialValues,
        [SearchParamsKeys.SHOW_SCHEDULED_ONLY]:
          searchParams.get(SearchParamsKeys.SHOW_SCHEDULED_ONLY) === "true" ? "true" : undefined,
      },
      onFiltersChange: handleFiltersChange,
    },
    streamQuery,
  };
};
