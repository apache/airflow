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
import { VStack } from "@chakra-ui/react";

import { FilterBar } from "src/components/FilterBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

export const BackfillsFilters = () => {
  const searchParamKeys: Array<FilterableSearchParamsKeys> = [
    SearchParamsKeys.START_DATE_RANGE,
    SearchParamsKeys.END_DATE_RANGE,
    SearchParamsKeys.CREATED_AT_RANGE,
    SearchParamsKeys.COMPLETED_AT_RANGE,
    SearchParamsKeys.MAX_ACTIVE_RUNS_GTE,
    SearchParamsKeys.MAX_ACTIVE_RUNS_LTE,
    SearchParamsKeys.REPROCESS_BEHAVIOR,
  ];

  const { filterConfigs, handleFiltersChange, initialValues } = useFiltersHandler(searchParamKeys);

  return (
    <VStack align="start" gap={4} paddingY="4px">
      <FilterBar
        configs={filterConfigs}
        initialValues={initialValues}
        onFiltersChange={handleFiltersChange}
      />
    </VStack>
  );
};
