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
  Flex,
  IconButton,
  ButtonGroup,
  createListCollection,
  type SelectValueChangeDetails,
  Popover,
  Portal,
  Select,
  VStack,
  Text,
  Box,
} from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiGrid } from "react-icons/fi";
import { LuKeyboard } from "react-icons/lu";
import { MdOutlineAccountTree } from "react-icons/md";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import type { DagRunType } from "openapi/requests/types.gen";
import { DagVersionSelect } from "src/components/DagVersionSelect";
import { directionOptions, type Direction } from "src/components/Graph/useGraphLayout";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import { SearchBar } from "src/components/SearchBar";
import { Button, Tooltip } from "src/components/ui";
import { Checkbox } from "src/components/ui/Checkbox";
import { dagRunTypeOptions } from "src/constants/stateOptions";

import { DagRunSelect } from "./DagRunSelect";
import { ToggleGroups } from "./ToggleGroups";

type Props = {
  readonly dagView: string;
  readonly limit: number;
  readonly panelGroupRef: React.RefObject<{ setLayout?: (layout: Array<number>) => void } & HTMLDivElement>;
  readonly runTypeFilter: DagRunType | undefined;
  readonly setDagView: (x: "graph" | "grid") => void;
  readonly setLimit: React.Dispatch<React.SetStateAction<number>>;
  readonly setRunTypeFilter: React.Dispatch<React.SetStateAction<DagRunType | undefined>>;
  readonly setShowGantt: React.Dispatch<React.SetStateAction<boolean>>;
  readonly setTriggeringUserFilter: React.Dispatch<React.SetStateAction<string | undefined>>;
  readonly showGantt: boolean;
  readonly triggeringUserFilter: string | undefined;
};

const getOptions = (translate: (key: string) => string) =>
  createListCollection({
    items: [
      { label: translate("dag:panel.dependencies.options.onlyTasks"), value: "tasks" },
      { label: translate("dag:panel.dependencies.options.externalConditions"), value: "immediate" },
      { label: translate("dag:panel.dependencies.options.allDagDependencies"), value: "all" },
    ],
  });

const displayRunOptions = createListCollection({
  items: [
    { label: "5", value: "5" },
    { label: "10", value: "10" },
    { label: "25", value: "25" },
    { label: "50", value: "50" },
    { label: "100", value: "100" },
    { label: "365", value: "365" },
  ],
});

const deps = ["all", "immediate", "tasks"];

type Dependency = (typeof deps)[number];

export const PanelButtons = ({
  dagView,
  limit,
  panelGroupRef,
  runTypeFilter,
  setDagView,
  setLimit,
  setRunTypeFilter,
  setShowGantt,
  setTriggeringUserFilter,
  showGantt,
  triggeringUserFilter,
}: Props) => {
  const { t: translate } = useTranslation(["components", "dag"]);
  const { dagId = "", runId } = useParams();
  const { fitView } = useReactFlow();
  const shouldShowToggleButtons = Boolean(runId);
  const [dependencies, setDependencies, removeDependencies] = useLocalStorage<Dependency>(
    `dependencies-${dagId}`,
    "tasks",
  );
  const [direction, setDirection] = useLocalStorage<Direction>(`direction-${dagId}`, "RIGHT");
  const handleLimitChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    const runLimit = Number(event.value[0]);

    setLimit(runLimit);
  };

  const handleDepsChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    if (event.value[0] === undefined || event.value[0] === "tasks" || !deps.includes(event.value[0])) {
      removeDependencies();
    } else {
      setDependencies(event.value[0]);
    }
  };

  const handleDirectionUpdate = (
    event: SelectValueChangeDetails<{ label: string; value: Array<string> }>,
  ) => {
    if (event.value[0] !== undefined) {
      setDirection(event.value[0] as Direction);
    }
  };

  const handleRunTypeChange = (event: SelectValueChangeDetails<string>) => {
    const [val] = event.value;

    if (val === undefined || val === "all") {
      setRunTypeFilter(undefined);
    } else {
      setRunTypeFilter(val as DagRunType);
    }
  };

  const handleTriggeringUserChange = (value: string) => {
    const trimmedValue = value.trim();

    setTriggeringUserFilter(trimmedValue === "" ? undefined : trimmedValue);
  };

  const handleFocus = (view: string) => {
    if (panelGroupRef.current) {
      const panelGroup = panelGroupRef.current;

      if (typeof panelGroup.setLayout === "function") {
        const newLayout = view === "graph" ? [70, 30] : [30, 70];

        panelGroup.setLayout(newLayout);
        // Used setTimeout to ensure DOM has been updated
        setTimeout(() => {
          void fitView();
        }, 1);
      }
    }
  };

  useHotkeys(
    "g",
    () => {
      if (dagView === "graph") {
        setDagView("grid");
        handleFocus("grid");
      } else {
        setDagView("graph");
        handleFocus("graph");
      }
    },
    [dagView],
    { preventDefault: true },
  );

  return (
    <Box position="absolute" top={1} width="100%" zIndex={1}>
      <Flex justifyContent="space-between">
        <ButtonGroup attached size="sm" variant="outline">
          <IconButton
            aria-label={translate("dag:panel.buttons.showGridShortcut")}
            colorPalette="brand"
            onClick={() => {
              setDagView("grid");
              if (dagView === "grid") {
                handleFocus("grid");
              }
            }}
            title={translate("dag:panel.buttons.showGridShortcut")}
            variant={dagView === "grid" ? "solid" : "outline"}
          >
            <FiGrid />
          </IconButton>
          <IconButton
            aria-label={translate("dag:panel.buttons.showGraphShortcut")}
            colorPalette="brand"
            onClick={() => {
              setDagView("graph");
              if (dagView === "graph") {
                handleFocus("graph");
              }
            }}
            title={translate("dag:panel.buttons.showGraphShortcut")}
            variant={dagView === "graph" ? "solid" : "outline"}
          >
            <MdOutlineAccountTree />
          </IconButton>
        </ButtonGroup>
        <Flex gap={1} mr={3}>
          <ToggleGroups />
          {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
          <Popover.Root autoFocus={false} positioning={{ placement: "bottom-end" }}>
            <Popover.Trigger asChild>
              <Button size="sm" variant="outline">
                {translate("dag:panel.buttons.options")}
                <FiChevronDown size="0.5rem" />
              </Button>
            </Popover.Trigger>
            <Portal>
              <Popover.Positioner>
                <Popover.Content>
                  <Popover.Arrow />
                  <Popover.Body display="flex" flexDirection="column" gap={4} p={2}>
                    {dagView === "graph" ? (
                      <>
                        <DagVersionSelect />
                        <DagRunSelect limit={limit} />

                        <Select.Root
                          // @ts-expect-error The expected option type is incorrect
                          collection={getOptions(translate)}
                          data-testid="dependencies"
                          onValueChange={handleDepsChange}
                          size="sm"
                          value={[dependencies]}
                        >
                          <Select.Label fontSize="xs">
                            {translate("dag:panel.dependencies.label")}
                          </Select.Label>
                          <Select.Control>
                            <Select.Trigger>
                              <Select.ValueText placeholder={translate("dag:panel.dependencies.label")} />
                            </Select.Trigger>
                            <Select.IndicatorGroup>
                              <Select.Indicator />
                            </Select.IndicatorGroup>
                          </Select.Control>
                          <Select.Positioner>
                            <Select.Content>
                              {getOptions(translate).items.map((option) => (
                                <Select.Item item={option} key={option.value}>
                                  {option.label}
                                </Select.Item>
                              ))}
                            </Select.Content>
                          </Select.Positioner>
                        </Select.Root>

                        <Select.Root
                          // @ts-expect-error The expected option type is incorrect
                          collection={directionOptions(translate)}
                          onValueChange={handleDirectionUpdate}
                          size="sm"
                          value={[direction]}
                        >
                          <Select.Label fontSize="xs">
                            {translate("dag:panel.graphDirection.label")}
                          </Select.Label>
                          <Select.Control>
                            <Select.Trigger>
                              <Select.ValueText />
                            </Select.Trigger>
                            <Select.IndicatorGroup>
                              <Select.Indicator />
                            </Select.IndicatorGroup>
                          </Select.Control>
                          <Select.Positioner>
                            <Select.Content>
                              {directionOptions(translate).items.map((option) => (
                                <Select.Item item={option} key={option.value}>
                                  {option.label}
                                </Select.Item>
                              ))}
                            </Select.Content>
                          </Select.Positioner>
                        </Select.Root>
                      </>
                    ) : (
                      <>
                        <Select.Root
                          // @ts-expect-error The expected option type is incorrect
                          collection={displayRunOptions}
                          data-testid="display-dag-run-options"
                          onValueChange={handleLimitChange}
                          size="sm"
                          value={[limit.toString()]}
                        >
                          <Select.Label>{translate("dag:panel.dagRuns.label")}</Select.Label>
                          <Select.Control>
                            <Select.Trigger>
                              <Select.ValueText />
                            </Select.Trigger>
                            <Select.IndicatorGroup>
                              <Select.Indicator />
                            </Select.IndicatorGroup>
                          </Select.Control>
                          <Select.Positioner>
                            <Select.Content>
                              {displayRunOptions.items.map((option) => (
                                <Select.Item item={option} key={option.value}>
                                  {option.label}
                                </Select.Item>
                              ))}
                            </Select.Content>
                          </Select.Positioner>
                        </Select.Root>
                        <Select.Root
                          // @ts-expect-error The expected option type is incorrect
                          collection={dagRunTypeOptions}
                          data-testid="run-type-filter"
                          onValueChange={handleRunTypeChange}
                          size="sm"
                          value={[runTypeFilter ?? "all"]}
                        >
                          <Select.Label>{translate("common:dagRun.runType")}</Select.Label>
                          <Select.Control>
                            <Select.Trigger>
                              <Select.ValueText>
                                {(runTypeFilter ?? "all") === "all" ? (
                                  translate("dags:filters.allRunTypes")
                                ) : (
                                  <Flex gap={1}>
                                    <RunTypeIcon runType={runTypeFilter!} />
                                    {translate(
                                      dagRunTypeOptions.items.find((item) => item.value === runTypeFilter)
                                        ?.label ?? "",
                                    )}
                                  </Flex>
                                )}
                              </Select.ValueText>
                            </Select.Trigger>
                            <Select.IndicatorGroup>
                              <Select.Indicator />
                            </Select.IndicatorGroup>
                          </Select.Control>
                          <Select.Positioner>
                            <Select.Content>
                              {dagRunTypeOptions.items.map((option) => (
                                <Select.Item item={option} key={option.value}>
                                  {option.value === "all" ? (
                                    translate(option.label)
                                  ) : (
                                    <Flex gap={1}>
                                      <RunTypeIcon runType={option.value as DagRunType} />
                                      {translate(option.label)}
                                    </Flex>
                                  )}
                                </Select.Item>
                              ))}
                            </Select.Content>
                          </Select.Positioner>
                        </Select.Root>
                        <VStack alignItems="flex-start">
                          <Text fontSize="xs" mb={1}>
                            {translate("common:dagRun.triggeringUser")}
                          </Text>
                          <SearchBar
                            defaultValue={triggeringUserFilter ?? ""}
                            hideAdvanced
                            hotkeyDisabled
                            onChange={handleTriggeringUserChange}
                            placeHolder={translate("common:dagRun.triggeringUser")}
                          />
                        </VStack>
                        {shouldShowToggleButtons ? (
                          <VStack alignItems="flex-start" px={1}>
                            <Checkbox checked={showGantt} onChange={() => setShowGantt(!showGantt)} size="sm">
                              {translate("dag:panel.buttons.showGantt")}
                            </Checkbox>
                          </VStack>
                        ) : undefined}
                      </>
                    )}
                  </Popover.Body>
                </Popover.Content>
              </Popover.Positioner>
            </Portal>
          </Popover.Root>
        </Flex>
      </Flex>

      {dagView === "grid" && (
        <Flex color="fg.muted" justifyContent="flex-end" mt={1}>
          <Tooltip
            content={
              <Box>
                <Text>{translate("dag:navigation.navigation", { arrow: "↑↓←→" })}</Text>
                <Text>{translate("dag:navigation.toggleGroup")}</Text>
              </Box>
            }
          >
            <LuKeyboard />
          </Tooltip>
        </Flex>
      )}
    </Box>
  );
};
