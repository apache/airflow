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
  Box,
  createListCollection,
  Flex,
  Popover,
  Portal,
  Select,
  type SelectValueChangeDetails,
  Text,
  VStack,
} from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { useEffect, useRef } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiGrid } from "react-icons/fi";
import { LuChartGantt, LuKeyboard } from "react-icons/lu";
import { MdSettings } from "react-icons/md";
import type { ImperativePanelGroupHandle } from "react-resizable-panels";
import { useParams } from "react-router-dom";

import { DagVersionSelect } from "src/components/DagVersionSelect";
import { IconButton, Tooltip } from "src/components/ui";
import { type ButtonGroupOption, ButtonGroupToggle } from "src/components/ui/ButtonGroupToggle";
import type { DagView } from "src/constants/dagView";
import type { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { useContainerWidth } from "src/utils/useContainerWidth";

import { DagRunSelect } from "./DagRunSelect";
import { RunTypeLegend } from "./Grid/RunTypeLegend";
import { GridFilters } from "./GridFilters";
import { TaskStreamFilter } from "./TaskStreamFilter";
import { ToggleGroups } from "./ToggleGroups";
import { VersionIndicatorSelect } from "./VersionIndicatorSelect";

type Props = {
  readonly dagView: DagView;
  readonly limit: number;
  readonly panelGroupRef: React.RefObject<ImperativePanelGroupHandle | null>;
  readonly setDagView: (value: DagView) => void;
  readonly setLimit: (value: number) => void;
  readonly setShowVersionIndicatorMode: React.Dispatch<React.SetStateAction<VersionIndicatorOptions>>;
  readonly showVersionIndicatorMode: VersionIndicatorOptions;
};

const getWidthBasedConfig = (width: number, enableResponsiveOptions: boolean) => {
  const breakpoints = enableResponsiveOptions
    ? [
        { limit: 100, min: 1600, options: ["1", "5", "10", "25", "50"] }, // xl: extra large screens
        { limit: 25, min: 1024, options: ["1", "5", "10", "25"] }, // lg: large screens
        { limit: 10, min: 384, options: ["1", "5", "10"] }, // md: medium screens
        { limit: 5, min: 0, options: ["1", "5"] }, // sm: small screens and below
      ]
    : [{ limit: 5, min: 0, options: ["1", "5", "10", "25", "50"] }];

  const config = breakpoints.find(({ min }) => width >= min) ?? breakpoints[breakpoints.length - 1];

  return {
    displayRunOptions: createListCollection({
      items: config?.options.map((value) => ({ label: value, value })) ?? [],
    }),
    limit: config?.limit ?? 5,
  };
};

  const { t: translate } = useTranslation(["common", "components", "dag"]);
  const { runId } = useParams();
  const { fitView } = useReactFlow();
  const shouldShowToggleButtons = Boolean(runId);
  const containerRef = useRef<HTMLDivElement>(null);
  const containerWidth = useContainerWidth(containerRef);
  const handleLimitChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    const runLimit = Number(event.value[0]);

    setLimit(runLimit);
  };

  const enableResponsiveOptions = dagView === "gantt";

  const { displayRunOptions, limit: defaultLimit } = getWidthBasedConfig(
    containerWidth,
    enableResponsiveOptions,
  );

  useEffect(() => {
    if (enableResponsiveOptions && limit > defaultLimit) {
      setLimit(defaultLimit);
    }
  }, [defaultLimit, enableResponsiveOptions, limit, setLimit]);

  const handleFocus = (view: string) => {
    if (panelGroupRef.current) {
      const newLayout = view === "graph" ? [70, 30] : [30, 70];

      panelGroupRef.current.setLayout(newLayout);
      // Used setTimeout to ensure DOM has been updated
      setTimeout(() => {
        void fitView();
      }, 1);
    }
  };

  const dagViewOptions: Array<ButtonGroupOption<DagView>> = [
    {
      dataTestId: "grid-view-button",
      label: <FiGrid />,
      title: translate("dag:panel.buttons.showGridShortcut"),
      value: "grid",
    },
    ...(shouldShowToggleButtons
      ? [
          {
            label: <LuChartGantt />,
            title: translate("dag:panel.buttons.showGantt"),
            value: "gantt" as const,
          },
        ]
      : []),
  ];

  const handleDagViewChange = (view: DagView) => {
    if (view === dagView) {
      handleFocus(view);
    } else {
      setDagView(view);
    }
  };

  useHotkeys(
    "g",
    () => {
      if (shouldShowToggleButtons) {
        const newView = dagView === "gantt" ? "grid" : "gantt";

        setDagView(newView);
        handleFocus(newView);
      }
    },
    [dagView, shouldShowToggleButtons],
    { preventDefault: true },
  );

  return (
    <Box bg="bg" pr={4} ref={containerRef} width="100%" zIndex={1}>
      <Flex justifyContent="space-between">
        <ButtonGroupToggle isIcon onChange={handleDagViewChange} options={dagViewOptions} value={dagView} />
        <Flex alignItems="center" gap={1} justifyContent="space-between">
          <ToggleGroups />
          <TaskStreamFilter />
          {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
          <Popover.Root autoFocus={false} positioning={{ placement: "bottom-end" }}>
            <Popover.Trigger asChild>
              <IconButton label={translate("dag:panel.buttons.options")}>
                <MdSettings />
              </IconButton>
            </Popover.Trigger>
            <Portal>
              <Popover.Positioner>
                <Popover.Content>
                  <Popover.Arrow />
                  <Popover.Body
                    display="flex"
                    flexDirection="column"
                    gap={4}
                    maxH="70vh"
                    overflowY="auto"
                    p={2}
                  >
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
                    <VStack alignItems="flex-start" px={1}>
                      <VersionIndicatorSelect
                        onChange={setShowVersionIndicatorMode}
                        value={showVersionIndicatorMode}
                      />
                    </VStack>
                  </Popover.Body>
                </Popover.Content>
              </Popover.Positioner>
            </Portal>
          </Popover.Root>
        </Flex>
      </Flex>

      <Flex justifyContent="space-between" mt={1}>
        <GridFilters />
        <Flex color="fg.muted" gap={2} justifyContent="flex-end" mt={1}>
          <RunTypeLegend />
          <Tooltip
            content={
              <Box>
                <Text>{translate("dag:navigation.navigation", { arrow: "↑↓←→" })}</Text>
                <Text>{translate("dag:navigation.toggleGroup")}</Text>
              </Box>
            }
            portalled
          >
            <LuKeyboard />
          </Tooltip>
        </Flex>
      </Flex>
    </Box>
  );
};
