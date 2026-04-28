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
import { useEffect, useRef, useState } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiSearch } from "react-icons/fi";
import { useParams, useSearchParams } from "react-router-dom";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { AttrSelectFilterMulti } from "src/components/AttrSelectFilterMulti";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { Menu } from "src/components/ui/Menu";
import { NumberInputField, NumberInputRoot } from "src/components/ui/NumberInput";
import { SearchParamsKeys } from "src/constants/searchParams";
import { taskInstanceStateOptions } from "src/constants/stateOptions";
import { useGroups } from "src/context/groups";

export const GraphTaskFilters = () => {
  const { t: translate } = useTranslation(["dag", "tasks"]);
  const { runId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const { allGroupIds: allTaskGroups, allOperators } = useGroups();

  const selectedOperators = searchParams.getAll(SearchParamsKeys.GRAPH_OPERATOR);
  const selectedTaskGroups = searchParams.getAll(SearchParamsKeys.GRAPH_TASK_GROUP);
  const selectedStates = searchParams.getAll(SearchParamsKeys.GRAPH_TASK_STATE);
  const mapIndexParam = searchParams.get(SearchParamsKeys.GRAPH_MAP_INDEX) ?? "";
  const durationGteParam = searchParams.get(SearchParamsKeys.GRAPH_DURATION_GTE) ?? "";

  const hasActiveFilters =
    selectedOperators.length > 0 ||
    selectedTaskGroups.length > 0 ||
    selectedStates.length > 0 ||
    mapIndexParam !== "" ||
    durationGteParam !== "";

  const stateItems = taskInstanceStateOptions.items.filter(
    (item) => item.value !== "all" && item.value !== "none",
  );

  const allGraphFilterKeys = [
    SearchParamsKeys.GRAPH_OPERATOR,
    SearchParamsKeys.GRAPH_TASK_GROUP,
    SearchParamsKeys.GRAPH_TASK_STATE,
    SearchParamsKeys.GRAPH_MAP_INDEX,
    SearchParamsKeys.GRAPH_DURATION_GTE,
  ];

  const handleMultiChange = (key: string) => (values: Array<string> | undefined) => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        next.delete(key);
        values?.forEach((val) => next.append(key, val));

        return next;
      },
      { replace: true },
    );
  };

  const handleNumberChange =
    (key: string, condition: (n: number) => boolean) => (details: NumberInputValueChangeDetails) => {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);

          if (condition(details.valueAsNumber)) {
            next.set(key, details.value);
          } else {
            next.delete(key);
          }

          return next;
        },
        { replace: true },
      );
    };

  const clearAllFilters = () => {
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        allGraphFilterKeys.forEach((key) => next.delete(key));

        return next;
      },
      { replace: true },
    );
  };

  const isInitialMount = useRef(true);

  useEffect(() => {
    if (isInitialMount.current) {
      isInitialMount.current = false;

      return;
    }
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);

        allGraphFilterKeys.forEach((key) => next.delete(key));

        return next;
      },
      { replace: true },
    );
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [allOperators, allTaskGroups]);

  const [isOpen, setIsOpen] = useState(false);

  useHotkeys("mod+shift+f", () => setIsOpen(true), { preventDefault: true });

  const panelTitle = translate("dag:panel.graphFilters.title");

  return (
    <Menu.Root
      onOpenChange={({ open: nextOpen }) => setIsOpen(nextOpen)}
      open={isOpen}
      positioning={{ placement: "bottom-end" }}
    >
      <Menu.Trigger asChild>
        <IconButton
          aria-label={panelTitle}
          colorPalette="brand"
          size="md"
          title={panelTitle}
          variant={hasActiveFilters ? "solid" : "ghost"}
        >
          <FiSearch />
        </IconButton>
      </Menu.Trigger>
      <Portal>
        <Menu.Positioner>
          <Menu.Content alignItems="start" display="flex" flexDirection="column" gap={2} p={4}>
            <Text fontSize="sm" fontWeight="semibold">
              {panelTitle}
            </Text>

            <Separator my={2} />

            {allOperators.length > 1 ? (
              <VStack alignItems="flex-start" width="100%">
                <Text fontSize="xs">{translate("tasks:selectOperator")}</Text>
                <AttrSelectFilterMulti
                  displayPrefix={undefined}
                  handleSelect={handleMultiChange(SearchParamsKeys.GRAPH_OPERATOR)}
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
                  handleSelect={handleMultiChange(SearchParamsKeys.GRAPH_TASK_GROUP)}
                  placeholderText={translate("dag:panel.graphFilters.selectTaskGroup")}
                  selectedValues={selectedTaskGroups}
                  values={allTaskGroups}
                />
              </VStack>
            ) : undefined}

            {runId === undefined ? undefined : (
              <>
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("dag:panel.graphFilters.selectStatus")}</Text>
                  <Select.Root
                    collection={taskInstanceStateOptions}
                    multiple
                    onValueChange={({ value }) => handleMultiChange(SearchParamsKeys.GRAPH_TASK_STATE)(value)}
                    value={selectedStates}
                  >
                    <Select.Trigger colorPalette="brand" minW="max-content">
                      <Select.ValueText
                        placeholder={translate("dag:panel.graphFilters.selectStatus")}
                        width="auto"
                      >
                        {() => selectedStates.join(", ") || undefined}
                      </Select.ValueText>
                    </Select.Trigger>
                    <Select.Content>
                      {stateItems.map((option) => (
                        <Select.Item item={option} key={option.value}>
                          <StateBadge state={option.value as TaskInstanceState}>
                            {translate(option.label)}
                          </StateBadge>
                        </Select.Item>
                      ))}
                    </Select.Content>
                  </Select.Root>
                </VStack>
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("dag:panel.graphFilters.mapIndex")}</Text>
                  <NumberInputRoot
                    min={0}
                    onValueChange={handleNumberChange(SearchParamsKeys.GRAPH_MAP_INDEX, (num) => num >= 0)}
                    size="sm"
                    value={mapIndexParam}
                    w="100%"
                  >
                    <NumberInputField placeholder={translate("dag:panel.graphFilters.mapIndex")} />
                  </NumberInputRoot>
                  <Text color="fg.muted" fontSize="xs">
                    {translate("dag:panel.graphFilters.mapIndexHint")}
                  </Text>
                </VStack>
                <VStack alignItems="flex-start" width="100%">
                  <Text fontSize="xs">{translate("dag:panel.graphFilters.durationGte")}</Text>
                  <NumberInputRoot
                    min={0}
                    onValueChange={handleNumberChange(SearchParamsKeys.GRAPH_DURATION_GTE, (num) => num > 0)}
                    size="sm"
                    step={0.1}
                    value={durationGteParam}
                    w="100%"
                  >
                    <NumberInputField placeholder={translate("dag:panel.graphFilters.durationGte")} />
                  </NumberInputRoot>
                  <Text color="fg.muted" fontSize="xs">
                    {translate("dag:panel.graphFilters.durationGteHint")}
                  </Text>
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
