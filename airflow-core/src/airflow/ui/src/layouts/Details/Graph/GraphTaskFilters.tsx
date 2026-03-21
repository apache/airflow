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
import { HStack, type NumberInputValueChangeDetails } from "@chakra-ui/react";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import type { NodeResponse } from "openapi/requests/types.gen";
import { ResetButton } from "src/components/ui";
import { NumberInputField, NumberInputRoot } from "src/components/ui/NumberInput";
import { SearchParamsKeys } from "src/constants/searchParams";
import { taskInstanceStateOptions } from "src/constants/stateOptions";
import { AttrSelectFilter } from "src/pages/Dag/Tasks/TaskFilters/AttrSelectFilter";
import { AttrSelectFilterMulti } from "src/pages/Dag/Tasks/TaskFilters/AttrSelectFilterMulti";

const collectOperators = (nodes: Array<NodeResponse>): Array<string> => {
  const operators = new Set<string>();

  const walk = (nodeList: Array<NodeResponse>) => {
    for (const node of nodeList) {
      if (node.operator !== undefined && node.operator !== null) {
        operators.add(node.operator);
      }
      if (node.children) {
        walk(node.children);
      }
    }
  };

  walk(nodes);

  return [...operators].sort();
};

const collectTaskGroups = (nodes: Array<NodeResponse>): Array<string> => {
  const groups: Array<string> = [];

  const walk = (nodeList: Array<NodeResponse>) => {
    for (const node of nodeList) {
      if (node.children) {
        groups.push(node.id);
        walk(node.children);
      }
    }
  };

  walk(nodes);

  return groups.sort();
};

type Props = {
  readonly graphNodes: Array<NodeResponse>;
};

const graphFilterKeys = [
  SearchParamsKeys.OPERATOR,
  SearchParamsKeys.TASK_GROUP,
  SearchParamsKeys.STATE,
  SearchParamsKeys.MAPPED,
  SearchParamsKeys.MAP_INDEX,
  SearchParamsKeys.DURATION_GTE,
];

export const GraphTaskFilters = ({ graphNodes }: Props) => {
  const { t: translate } = useTranslation(["dag", "tasks", "common"]);
  const { runId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const selectedOperators = searchParams.getAll(SearchParamsKeys.OPERATOR);
  const selectedTaskGroups = searchParams.getAll(SearchParamsKeys.TASK_GROUP);
  const selectedStates = searchParams.getAll(SearchParamsKeys.STATE);
  const selectedMapped = searchParams.get(SearchParamsKeys.MAPPED) ?? undefined;
  const mapIndexParam = searchParams.get(SearchParamsKeys.MAP_INDEX) ?? "";
  const durationGteParam = searchParams.get(SearchParamsKeys.DURATION_GTE) ?? "";

  const allOperators = useMemo(() => collectOperators(graphNodes), [graphNodes]);
  const allTaskGroups = useMemo(() => collectTaskGroups(graphNodes), [graphNodes]);

  const stateValues = useMemo(
    () => taskInstanceStateOptions.items.filter((item) => item.value !== "all").map((item) => item.value),
    [],
  );

  const mappedValues = [
    { key: "true", label: translate("tasks:mapped") },
    { key: "false", label: translate("tasks:notMapped") },
  ];

  const activeFilterCount = graphFilterKeys.reduce((count, key) => {
    const values = searchParams.getAll(key);

    return count + values.filter((val) => val !== "").length;
  }, 0);

  const handleOperatorsChange = (values: Array<string> | undefined) => {
    searchParams.delete(SearchParamsKeys.OPERATOR);
    values?.forEach((val) => searchParams.append(SearchParamsKeys.OPERATOR, val));
    setSearchParams(searchParams);
  };

  const handleTaskGroupsChange = (values: Array<string> | undefined) => {
    searchParams.delete(SearchParamsKeys.TASK_GROUP);
    values?.forEach((val) => searchParams.append(SearchParamsKeys.TASK_GROUP, val));
    setSearchParams(searchParams);
  };

  const handleStatesChange = (values: Array<string> | undefined) => {
    searchParams.delete(SearchParamsKeys.STATE);
    values?.forEach((val) => searchParams.append(SearchParamsKeys.STATE, val));
    setSearchParams(searchParams);
  };

  const handleMappedChange = (value: string | undefined) => {
    searchParams.delete(SearchParamsKeys.MAPPED);
    if (value !== undefined) {
      searchParams.set(SearchParamsKeys.MAPPED, value);
    }
    setSearchParams(searchParams);
  };

  const handleMapIndexChange = (details: NumberInputValueChangeDetails) => {
    if (details.valueAsNumber >= 0) {
      searchParams.set(SearchParamsKeys.MAP_INDEX, details.value);
    } else {
      searchParams.delete(SearchParamsKeys.MAP_INDEX);
    }
    setSearchParams(searchParams);
  };

  const handleDurationChange = (details: NumberInputValueChangeDetails) => {
    if (details.valueAsNumber > 0) {
      searchParams.set(SearchParamsKeys.DURATION_GTE, details.value);
    } else {
      searchParams.delete(SearchParamsKeys.DURATION_GTE);
    }
    setSearchParams(searchParams);
  };

  const handleClearFilters = () => {
    for (const key of graphFilterKeys) {
      searchParams.delete(key);
    }
    setSearchParams(searchParams);
  };

  // Don't render if there's nothing to filter
  if (allOperators.length <= 1 && allTaskGroups.length === 0) {
    return undefined;
  }

  return (
    <HStack gap={2}>
      {allOperators.length > 1 ? (
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleOperatorsChange}
          placeholderText={translate("tasks:selectOperator")}
          selectedValues={selectedOperators}
          values={allOperators}
        />
      ) : undefined}
      {allTaskGroups.length > 0 ? (
        <AttrSelectFilterMulti
          displayPrefix={undefined}
          handleSelect={handleTaskGroupsChange}
          placeholderText={translate("dag:panel.graphFilters.selectTaskGroup")}
          selectedValues={selectedTaskGroups}
          values={allTaskGroups}
        />
      ) : undefined}
      {runId === undefined ? undefined : (
        <>
          <AttrSelectFilterMulti
            displayPrefix={undefined}
            handleSelect={handleStatesChange}
            placeholderText={translate("dag:panel.graphFilters.selectStatus")}
            selectedValues={selectedStates}
            values={stateValues}
          />
          <NumberInputRoot
            min={0}
            onValueChange={handleMapIndexChange}
            size="sm"
            value={mapIndexParam}
            w="270px"
          >
            <NumberInputField placeholder={translate("dag:panel.graphFilters.mapIndex")} />
          </NumberInputRoot>
          <NumberInputRoot
            min={0}
            onValueChange={handleDurationChange}
            size="sm"
            value={durationGteParam}
            w="270px"
          >
            <NumberInputField placeholder={translate("dag:panel.graphFilters.durationGte")} />
          </NumberInputRoot>
        </>
      )}
      <AttrSelectFilter
        handleSelect={handleMappedChange}
        placeholderText={translate("tasks:selectMapped")}
        selectedValue={selectedMapped}
        values={mappedValues}
      />
      <ResetButton filterCount={activeFilterCount} onClearFilters={handleClearFilters} />
    </HStack>
  );
};
