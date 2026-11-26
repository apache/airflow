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

import { GANTT_DATE_FILTER_KEYS, GRID_FILTER_KEYS } from "src/constants/persistentSearchParams";
import { useFiltersHandler } from "src/utils";

type Props = {
  readonly showGanttDateFilters?: boolean;
};

export const GridFilters = ({ showGanttDateFilters = false }: Props) => {
  const searchParamKeys = [...GRID_FILTER_KEYS, ...(showGanttDateFilters ? GANTT_DATE_FILTER_KEYS : [])];

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);

  return (
    <FilterBar
      configs={filterConfigs}
      initialValues={initialValues}
      onFiltersChange={handleFiltersChange}
      showPresetFilters={false}
    />
  );
};
