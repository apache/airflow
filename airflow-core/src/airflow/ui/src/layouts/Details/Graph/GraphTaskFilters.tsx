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
import {
  Button,
  IconButton,
  type NumberInputValueChangeDetails,
  Portal,
  Separator,
  Text,
  VStack,
} from "@chakra-ui/react";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { FiSearch } from "react-icons/fi";
import { useParams, useSearchParams } from "react-router-dom";

import { useStructureServiceStructureData } from "openapi/queries";
import type { NodeResponse } from "openapi/requests/types.gen";
import { Menu } from "src/components/ui/Menu";
import { NumberInputField, NumberInputRoot } from "src/components/ui/NumberInput";
import { SearchParamsKeys } from "src/constants/searchParams";
import { taskInstanceStateOptions } from "src/constants/stateOptions";
import useSelectedVersion from "src/hooks/useSelectedVersion";
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

export const GraphTaskFilters = () => {
  const { t: translate } = useTranslation(["dag", "tasks"]);
  const { dagId = "", runId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedVersion = useSelectedVersion();

  const { data: graphData } = useStructureServiceStructureData(
    { dagId, versionNumber: selectedVersion },
    undefined,
    { enabled: selectedVersion !== undefined },
  );
  const graphNodes = useMemo(() => graphData?.nodes ?? [], [graphData?.nodes]);
  const allOperators = useMemo(() => collectOperators(graphNodes), [graphNodes]);
  const allTaskGroups = useMemo(() => collectTaskGroups(graphNodes), [graphNodes]);

  const selectedOperators = searchParams.getAll(SearchParamsKeys.OPERATOR);
  const selectedTaskGroups = searchParams.getAll(SearchParamsKeys.TASK_GROUP);
  const selectedStates = searchParams.getAll(SearchParamsKeys.STATE);
  const selectedMapped = searchParams.get(SearchParamsKeys.MAPPED) ?? undefined;
  const mapIndexParam = searchParams.get(SearchParamsKeys.MAP_INDEX) ?? "";
  const durationGteParam = searchParams.get(SearchParamsKeys.DURATION_GTE) ?? "";

  const mapIndexIsSet = mapIndexParam !== "";

  const hasActiveFilters =
    selectedOperators.length > 0 ||
    selectedTaskGroups.length > 0 ||
    selectedStates.length > 0 ||
    selectedMapped !== undefined ||
    mapIndexIsSet ||
    durationGteParam !== "";

  const stateValues = useMemo(
    () => taskInstanceStateOptions.items.filter((item) => item.value !== "all").map((item) => item.value),
    [],
  );

  const mappedValues = [
    { key: "true", label: translate("tasks:mapped") },
    { key: "false", label: translate("tasks:notMapped") },
  ];

  const handleMultiChange = (key: string) => (values: Array<string> | undefined) => {
    searchParams.delete(key);
    values?.forEach((val) => searchParams.append(key, val));
    setSearchParams(searchParams);
  };

  const handleSingleChange = (key: string) => (value: string | undefined) => {
    searchParams.delete(key);
    if (value !== undefined) {
      searchParams.set(key, value);
    }
    setSearchParams(searchParams);
  };

  const handleNumberChange =
    (key: string, condition: (n: number) => boolean) => (details: NumberInputValueChangeDetails) => {
      if (condition(details.valueAsNumber)) {
        searchParams.set(key, details.value);
      } else {
        searchParams.delete(key);
      }
      setSearchParams(searchParams);
    };

  const clearAllFilters = () => {
    [
      SearchParamsKeys.OPERATOR,
      SearchParamsKeys.TASK_GROUP,
      SearchParamsKeys.STATE,
      SearchParamsKeys.MAPPED,
      SearchParamsKeys.MAP_INDEX,
      SearchParamsKeys.DURATION_GTE,
    ].forEach((key) => searchParams.delete(key));
    setSearchParams(searchParams);
  };

  const tooltipContent = translate("dag:panel.graphFilters.title");

  return (
    <Menu.Root positioning={{ placement: "bottom-end" }}>
      <Menu.Trigger asChild>
        <IconButton
          aria-label={tooltipContent}
          colorPalette="brand"
          size="md"
          title={tooltipContent}
          variant={hasActiveFilters ? "solid" : "ghost"}
        >
          <FiSearch />
        </IconButton>
      </Menu.Trigger>
      <Portal>
        <Menu.Positioner>
          <Menu.Content alignItems="start" display="flex" flexDirection="column" gap={2} p={4}>
            <Text fontSize="sm" fontWeight="semibold">
              {translate("dag:panel.graphFilters.title")}
            </Text>

            <Separator my={2} />

            {allOperators.length > 1 ? (
              <VStack alignItems="flex-start" width="100%">
                <Text fontSize="xs">{translate("tasks:selectOperator")}</Text>
                <AttrSelectFilterMulti
                  displayPrefix={undefined}
                  handleSelect={handleMultiChange(SearchParamsKeys.OPERATOR)}
                  placeholderText={translate("tasks:selectOperator")}
                  selectedValues={selectedOperators}
                  values={allOperators}
                />
              </VStack>
            ) : undefined}

            {allTaskGroups.length > 0 ? (
              <VStack alignItems="flex-start" width="100%">
                <Text fontSize="xs">{translate("dag:panel.graphFilters.selectTaskGroup")}</Text>
                <AttrSelectFilterMulti
                  displayPrefix={undefined}
                  handleSelect={handleMultiChange(SearchParamsKeys.TASK_GROUP)}
                  placeholderText={translate("dag:panel.graphFilters.selectTaskGroup")}
                  selectedValues={selectedTaskGroups}
                  values={allTaskGroups}
                />
              </VStack>
            ) : undefined}

            {/* Hide mapped filter when mapIndex is set — mapIndex already implies mapped tasks */}
            {mapIndexIsSet ? undefined : (
              <VStack alignItems="flex-start" width="100%">
                <Text fontSize="xs">{translate("tasks:selectMapped")}</Text>
                <AttrSelectFilter
                  handleSelect={handleSingleChange(SearchParamsKeys.MAPPED)}
                  placeholderText={translate("tasks:selectMapped")}
                  selectedValue={selectedMapped}
                  values={mappedValues}
                />
              </VStack>
            )}

            {runId === undefined ? undefined : (
              <>
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("dag:panel.graphFilters.selectStatus")}</Text>
                  <AttrSelectFilterMulti
                    displayPrefix={undefined}
                    handleSelect={handleMultiChange(SearchParamsKeys.STATE)}
                    placeholderText={translate("dag:panel.graphFilters.selectStatus")}
                    selectedValues={selectedStates}
                    values={stateValues}
                  />
                </VStack>
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("dag:panel.graphFilters.mapIndex")}</Text>
                  <NumberInputRoot
                    min={0}
                    onValueChange={handleNumberChange(SearchParamsKeys.MAP_INDEX, (num) => num >= 0)}
                    size="sm"
                    value={mapIndexParam}
                    w="100%"
                  >
                    <NumberInputField placeholder={translate("dag:panel.graphFilters.mapIndex")} />
                  </NumberInputRoot>
                </VStack>
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("dag:panel.graphFilters.durationGte")}</Text>
                  <NumberInputRoot
                    min={0}
                    onValueChange={handleNumberChange(SearchParamsKeys.DURATION_GTE, (num) => num > 0)}
                    size="sm"
                    value={durationGteParam}
                    w="100%"
                  >
                    <NumberInputField placeholder={translate("dag:panel.graphFilters.durationGte")} />
                  </NumberInputRoot>
                </VStack>
              </>
            )}

            {hasActiveFilters ? (
              <Button onClick={clearAllFilters} size="sm" variant="outline" width="100%">
                {translate("dag:panel.graphFilters.clearFilters")}
              </Button>
            ) : undefined}
          </Menu.Content>
        </Menu.Positioner>
      </Portal>
    </Menu.Root>
  );
};
