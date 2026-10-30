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
import { FilterBar } from "src/components/FilterBar";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useConfig } from "src/queries/useConfig";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

import { usePausedDefault } from "../usePausedDefault";

export const DagsFilters = () => {
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  usePausedDefault();

  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.PAUSED,
    SearchParamsKeys.RUN_STATE,
    SearchParamsKeys.NEEDS_REVIEW,
    SearchParamsKeys.TAGS,
    SearchParamsKeys.OWNERS,
    SearchParamsKeys.TIMETABLE_TYPE,
    SearchParamsKeys.FAVORITE,
  ];

  if (multiTeamEnabled) {
    searchParamKeys.push(SearchParamsKeys.TEAMS);
  }

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);

  return (
    <FilterBar configs={filterConfigs} initialValues={initialValues} onFiltersChange={handleFiltersChange} />
  );
};
