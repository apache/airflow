/* eslint-disable max-lines */

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
  ButtonGroup,
  HStack,
  IconButton,
  Input,
  type NumberInputValueChangeDetails,
  Portal,
  Separator,
  Text,
  VStack,
} from "@chakra-ui/react";
import { useEffect, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { FiFilter, FiInfo } from "react-icons/fi";
import { useParams, useSearchParams } from "react-router-dom";

import { useStructureServiceStructureData } from "openapi/queries";
import type { NodeResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";
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

type Props = {
  readonly dagView: "graph" | "grid";
};

export const TaskStreamFilter = ({ dagView }: Props) => {
  const { t: translate } = useTranslation(["common", "components", "dag", "tasks"]);
  const { dagId = "", runId, taskId: currentTaskId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedVersion = useSelectedVersion();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";
  const depth = searchParams.get("depth") ?? undefined;
  const mode = searchParams.get("mode") ?? "static";

  // Graph task filters
  const { data: graphData } = useStructureServiceStructureData(
    { dagId, versionNumber: selectedVersion },
    undefined,
    { enabled: dagView === "graph" && selectedVersion !== undefined },
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

  const stateValues = useMemo(
    () => taskInstanceStateOptions.items.filter((item) => item.value !== "all").map((item) => item.value),
    [],
  );

  const mappedValues = [
    { key: "true", label: translate("tasks:mapped") },
    { key: "false", label: translate("tasks:notMapped") },
  ];

  const hasTaskFilters =
    dagView === "graph" &&
    (selectedOperators.length > 0 ||
      selectedTaskGroups.length > 0 ||
      selectedStates.length > 0 ||
      selectedMapped !== undefined ||
      mapIndexParam !== "" ||
      durationGteParam !== "");

  const hasActiveFilter = includeUpstream || includeDownstream || hasTaskFilters;
  const isCurrentTaskTheRoot = currentTaskId === filterRoot;
  const bothActive = isCurrentTaskTheRoot && includeUpstream && includeDownstream;
  const activeUpstream = isCurrentTaskTheRoot && includeUpstream && !includeDownstream;
  const activeDownstream = isCurrentTaskTheRoot && includeDownstream && !includeUpstream;

  // In traverse mode, update the root when the selected task changes
  useEffect(() => {
    if (
      mode === "traverse" &&
      hasActiveFilter &&
      currentTaskId !== undefined &&
      currentTaskId !== "" &&
      currentTaskId !== filterRoot
    ) {
      searchParams.set("root", currentTaskId);
      setSearchParams(searchParams, { replace: true });
    }
  }, [currentTaskId, mode, hasActiveFilter, filterRoot, searchParams, setSearchParams]);

  const buildFilterSearch = (options: {
    depth?: string;
    downstream: boolean;
    root?: string;
    upstream: boolean;
  }) => {
    const { depth: newDepth, downstream, root, upstream } = options;
    const hasDirection = upstream || downstream;
    const hasRoot = root !== undefined && root !== "";
    const hasDepth = newDepth !== undefined && newDepth !== "";

    if (upstream) {
      searchParams.set("upstream", "true");
    } else {
      searchParams.delete("upstream");
    }
    if (downstream) {
      searchParams.set("downstream", "true");
    } else {
      searchParams.delete("downstream");
    }
    if (hasRoot && hasDirection) {
      searchParams.set("root", root);
    } else {
      searchParams.delete("root");
      searchParams.delete("mode");
    }
    if (hasDepth && hasDirection) {
      searchParams.set("depth", newDepth);
    } else {
      searchParams.delete("depth");
    }
    setSearchParams(searchParams);
  };

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

  const clearAllTaskFilters = () => {
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

  const tooltipContent =
    filterRoot === undefined || !hasActiveFilter
      ? translate("dag:panel.taskStreamFilter.label")
      : `${filterRoot}: ${
          includeUpstream && includeDownstream
            ? translate("dag:panel.taskStreamFilter.options.both")
            : includeUpstream
              ? translate("dag:panel.taskStreamFilter.options.upstream")
              : translate("dag:panel.taskStreamFilter.options.downstream")
        }`;

  return (
    <Menu.Root positioning={{ placement: "bottom-end" }}>
      <Menu.Trigger asChild>
        <IconButton
          aria-label={tooltipContent}
          colorPalette="brand"
          size="md"
          title={tooltipContent}
          variant={hasActiveFilter ? "solid" : "ghost"}
        >
          <FiFilter />
        </IconButton>
      </Menu.Trigger>
      <Portal>
        <Menu.Positioner>
          <Menu.Content alignItems="start" display="flex" flexDirection="column" gap={2} p={4}>
            <Text fontSize="sm" fontWeight="semibold">
              {translate("dag:panel.taskStreamFilter.label")}
            </Text>

            {filterRoot !== undefined && hasActiveFilter ? (
              <Text color="brand.solid" fontSize="xs" fontWeight="medium">
                {translate("dag:panel.taskStreamFilter.activeFilter")}: {filterRoot} -{" "}
                {includeUpstream && includeDownstream
                  ? translate("dag:panel.taskStreamFilter.options.both")
                  : includeUpstream
                    ? translate("dag:panel.taskStreamFilter.options.upstream")
                    : translate("dag:panel.taskStreamFilter.options.downstream")}
              </Text>
            ) : undefined}

            {currentTaskId === undefined ? (
              <Text color="fg.muted" fontSize="xs">
                {translate("dag:panel.taskStreamFilter.clickTask")}
              </Text>
            ) : (
              <Text color="fg.muted" fontSize="xs">
                {translate("dag:panel.taskStreamFilter.selectedTask")}: <strong>{currentTaskId}</strong>
              </Text>
            )}

            <Separator my={2} />

            {/* Direction Section */}
            <VStack align="stretch" gap={2} width="100%">
              <Text fontSize="xs" fontWeight="semibold">
                {translate("dag:panel.taskStreamFilter.direction")}
              </Text>
              <VStack align="stretch" gap={1} width="100%">
                {[
                  { active: activeUpstream, down: false, key: "upstream", label: "upstream", up: true },
                  { active: activeDownstream, down: true, key: "downstream", label: "downstream", up: false },
                  { active: bothActive, down: true, key: "both", label: "both", up: true },
                ].map(({ active, down, key, label, up }) => {
                  const onClick = () =>
                    buildFilterSearch({ depth, downstream: down, root: currentTaskId, upstream: up });

                  return (
                    <Button
                      color={active ? "white" : undefined}
                      colorPalette={active ? "brand" : "gray"}
                      disabled={currentTaskId === undefined}
                      justifyContent="flex-start"
                      key={key}
                      onClick={onClick}
                      onKeyDown={(event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          event.preventDefault();
                          onClick();
                        }
                      }}
                      size="sm"
                      variant={active ? "solid" : "ghost"}
                      width="100%"
                    >
                      {translate(`dag:panel.taskStreamFilter.options.${label}`)}
                    </Button>
                  );
                })}
              </VStack>
            </VStack>

            <Separator my={2} />

            {/* Depth Section */}
            <VStack align="stretch" gap={2} width="100%">
              <Text fontSize="xs" fontWeight="semibold">
                {translate("dag:panel.taskStreamFilter.depth")}
              </Text>
              <Input
                disabled={currentTaskId === undefined || !hasActiveFilter}
                min={0}
                onChange={(event) => {
                  const { value } = event.target;

                  buildFilterSearch({
                    depth: value,
                    downstream: includeDownstream,
                    root: filterRoot,
                    upstream: includeUpstream,
                  });
                }}
                onKeyDown={(event) => {
                  event.stopPropagation();
                }}
                placeholder={translate("common:expression.all")}
                size="sm"
                type="number"
                value={depth ?? ""}
              />
            </VStack>

            <Separator my={2} />

            {/* Mode Section */}
            <VStack align="stretch" gap={2} width="100%">
              <HStack gap={1}>
                <Text fontSize="xs" fontWeight="semibold">
                  {translate("dag:panel.taskStreamFilter.mode")}
                </Text>
                <Tooltip
                  closeDelay={200}
                  content={translate("dag:panel.taskStreamFilter.modeTooltip")}
                  openDelay={0}
                >
                  <FiInfo size={12} />
                </Tooltip>
              </HStack>
              <ButtonGroup attached colorPalette="brand" size="sm" variant="outline" width="100%">
                <Button
                  disabled={!hasActiveFilter}
                  flex="1"
                  onClick={() => {
                    searchParams.set("mode", "static");
                    setSearchParams(searchParams);
                  }}
                  variant={mode === "static" ? "solid" : "outline"}
                >
                  {translate("dag:panel.taskStreamFilter.modes.static")}
                </Button>
                <Button
                  disabled={!hasActiveFilter}
                  flex="1"
                  onClick={() => {
                    searchParams.set("mode", "traverse");
                    setSearchParams(searchParams);
                  }}
                  variant={mode === "traverse" ? "solid" : "outline"}
                >
                  {translate("dag:panel.taskStreamFilter.modes.traverse")}
                </Button>
              </ButtonGroup>
            </VStack>

            <Separator my={2} />

            {(includeUpstream || includeDownstream) && filterRoot !== undefined ? (
              <Menu.Item asChild value="clear">
                <Button
                  onClick={() =>
                    buildFilterSearch({
                      depth: undefined,
                      downstream: false,
                      root: undefined,
                      upstream: false,
                    })
                  }
                  size="sm"
                  variant="outline"
                  width="100%"
                >
                  {translate("dag:panel.taskStreamFilter.clearFilter")}
                </Button>
              </Menu.Item>
            ) : undefined}

            {dagView === "graph" ? (
              <>
                <Separator my={2} />
                <Text fontSize="sm" fontWeight="semibold">
                  {translate("dag:panel.graphFilters.title")}
                </Text>
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
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("tasks:selectMapped")}</Text>
                  <AttrSelectFilter
                    handleSelect={handleSingleChange(SearchParamsKeys.MAPPED)}
                    placeholderText={translate("tasks:selectMapped")}
                    selectedValue={selectedMapped}
                    values={mappedValues}
                  />
                </VStack>
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
                {hasTaskFilters ? (
                  <Button onClick={clearAllTaskFilters} size="sm" variant="outline" width="100%">
                    {translate("dag:panel.graphFilters.clearFilters")}
                  </Button>
                ) : undefined}
              </>
            ) : undefined}
          </Menu.Content>
        </Menu.Positioner>
      </Portal>
    </Menu.Root>
  );
};
