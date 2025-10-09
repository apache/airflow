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
import { Box, HStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import type { TaskCollectionResponse } from "openapi/requests";
import { SearchBar } from "src/components/SearchBar.tsx";
import { ResetButton } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams.ts";
import { AttrSelectFilter } from "src/pages/Dag/Tasks/TaskFilters/AttrSelectFilter.tsx";
import { AttrSelectFilterMulti } from "src/pages/Dag/Tasks/TaskFilters/AttrSelectFilterMulti.tsx";

export const TaskFilters = ({ tasksData }: { readonly tasksData: TaskCollectionResponse | undefined }) => {
  const { MAPPED, NAME_PATTERN, OPERATOR, RETRIES, TRIGGER_RULE } = SearchParamsKeys;
  const { t: translate } = useTranslation("tasks");
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedOperators = searchParams.getAll(OPERATOR);
  const selectedTriggerRules = searchParams.getAll(TRIGGER_RULE);
  const selectedRetries = searchParams.getAll(RETRIES);
  const selectedMapped = searchParams.get(MAPPED) ?? undefined;

  const handleSelectedOperators = (value: Array<string> | undefined) => {
    searchParams.delete(OPERATOR);
    value?.forEach((x) => searchParams.append(OPERATOR, x));
    setSearchParams(searchParams);
  };
  const handleSelectedRetries = (value: Array<string> | undefined) => {
    searchParams.delete(RETRIES);
    value?.forEach((x) => searchParams.append(RETRIES, x));
    setSearchParams(searchParams);
  };
  const handleSelectedTriggerRules = (value: Array<string> | undefined) => {
    searchParams.delete(TRIGGER_RULE);
    value?.forEach((x) => searchParams.append(TRIGGER_RULE, x));
    setSearchParams(searchParams);
  };
  const handleSelectedMapped = (value: string | undefined) => {
    searchParams.delete(MAPPED);
    if (value !== undefined) {
      searchParams.set(MAPPED, value);
    }
    setSearchParams(searchParams);
  };

  const onClearFilters = () => {
    setSearchParams();
  };

  const allOperatorNames: Array<string> = [
    ...new Set(tasksData?.tasks.map((task) => task.operator_name).filter((item) => item !== null) ?? []),
  ];
  const allTriggerRules: Array<string> = [
    ...new Set(tasksData?.tasks.map((task) => task.trigger_rule).filter((item) => item !== null) ?? []),
  ];
  const allRetryValues: Array<string> = [
    ...new Set(
      tasksData?.tasks.map((task) => task.retries?.toString()).filter((item) => item !== undefined) ?? [],
    ),
  ];
  const allMappedValues = [
    { key: "true", label: translate("mapped") },
    { key: "false", label: translate("notMapped") },
  ];
  const taskNamePattern = searchParams.get(NAME_PATTERN) ?? "";
  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN, value);
    } else {
      searchParams.delete(NAME_PATTERN);
    }
    setSearchParams(searchParams);
  };

  return (
    <>
      <HStack justifyContent="space-between" style={{ marginBottom: "5px" }}>
        <SearchBar
          defaultValue={taskNamePattern}
          hideAdvanced
          hotkeyDisabled
          onChange={handleSearchChange}
          placeHolder={translate("searchTasks")}
        />
        <Box>
          <ResetButton filterCount={searchParams.size} onClearFilters={onClearFilters} />
        </Box>
      </HStack>
      <HStack justifyContent="space-between">
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleSelectedOperators}
          placeholderText={translate("selectOperator")}
          selectedValues={selectedOperators}
          values={allOperatorNames}
        />
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleSelectedTriggerRules}
          placeholderText={translate("selectTriggerRules")}
          selectedValues={selectedTriggerRules}
          values={allTriggerRules}
        />
        <AttrSelectFilterMulti
          displayPrefix={translate("retries")}
          handleSelect={handleSelectedRetries}
          placeholderText={translate("selectRetryValues")}
          selectedValues={selectedRetries}
          values={allRetryValues}
        />
        <AttrSelectFilter
          handleSelect={handleSelectedMapped}
          placeholderText={translate("selectMapped")}
          selectedValue={selectedMapped}
          values={allMappedValues}
        />
      </HStack>
    </>
  );
};
