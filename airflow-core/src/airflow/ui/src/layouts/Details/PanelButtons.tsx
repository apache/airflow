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
import type { Dispatch, RefObject, SetStateAction } from "react";

import { Box, Flex, Popover, Portal, Select, type SelectValueChangeDetails, VStack } from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { useTranslation } from "react-i18next";
import { FiGrid } from "react-icons/fi";
import { LuChartGantt } from "react-icons/lu";
import { MdOutlineAccountTree, MdSettings } from "react-icons/md";
import type { GroupImperativeHandle } from "react-resizable-panels";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import {
  IconButton,
  Switch,
  Tooltip,
  type ButtonGroupOption,
  ButtonGroupToggle,
} from "src/system-components";

import { DagVersionSelect } from "src/components/DagVersionSelect";
import { DirectionDropdown } from "src/components/Graph/DirectionDropdown";
import { GraphTaskFilters } from "src/components/GraphTaskFilters";

import type { DagView } from "src/constants/dagView";
import { SHOW_ALL_DEPENDENCIES_KEY } from "src/constants/localStorage";
import type { VersionIndicatorOptions } from "src/constants/showVersionIndicatorOptions";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";

import { DagRunSelect } from "./DagRunSelect";
import { RunTypeLegend } from "./Grid/RunTypeLegend";
import { GridFilters } from "./GridFilters";
import { TaskStreamFilter } from "./TaskStreamFilter";
import { ToggleGroups } from "./ToggleGroups";
import { VersionIndicatorSelect } from "./VersionIndicatorSelect";
import { getWidthBasedConfig } from "./runLimitConfig";

type Props = {
  readonly containerWidth: number;
  readonly dagView: DagView;
  readonly limit: number;
  readonly panelGroupRef: RefObject<GroupImperativeHandle | null>;
  readonly setDagView: (value: DagView) => void;
  readonly setLimit: (value: number) => void;
  readonly setShowVersionIndicatorMode: Dispatch<SetStateAction<VersionIndicatorOptions>>;
  readonly showVersionIndicatorMode: VersionIndicatorOptions;
};

/**
 * The options popover's trigger. Tooltip and popover each need their own element: both set an `id` on
 * whatever they wrap and zag resolves a trigger by id, so sharing one element leaves the loser unable
 * to find its anchor and positioning at the viewport origin.
 */
const OptionsTrigger = ({ label }: { readonly label: string }) => (
  <Tooltip content={label} portalled>
    <Box display="flex">
      <Popover.Trigger asChild>
        <IconButton aria-label={label} bg="bg" variant="outline">
          <MdSettings />
        </IconButton>
      </Popover.Trigger>
    </Box>
  </Tooltip>
);

export const PanelButtons = ({
  containerWidth,
  dagView,
  limit,
  panelGroupRef,
  setDagView,
  setLimit,
  setShowVersionIndicatorMode,
  showVersionIndicatorMode,
}: Props) => {
  const { t: translate } = useTranslation(["common", "components", "dag"]);
  const { dagId = "", runId } = useParams();
  const { fitView } = useReactFlow();
  const shouldShowToggleButtons = Boolean(runId);
  const [showAllDependencies, setShowAllDependencies] = useLocalStorage<boolean>(
    SHOW_ALL_DEPENDENCIES_KEY,
    false,
  );
  const handleLimitChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    const runLimit = Number(event.value[0]);

    setLimit(runLimit);
  };

  const enableResponsiveOptions = dagView === "gantt";

  const { displayRunOptions } = getWidthBasedConfig(containerWidth, enableResponsiveOptions);

  const handleFocus = (view: string) => {
    if (panelGroupRef.current) {
      const newLayout =
        view === "graph"
          ? { "details-panel": 30, "main-panel": 70 }
          : { "details-panel": 70, "main-panel": 30 };

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
    {
      label: <MdOutlineAccountTree />,
      title: translate("dag:panel.buttons.showGraphShortcut"),
      value: "graph",
    },
  ];

  const handleDagViewChange = (view: DagView) => {
    if (view === dagView) {
      handleFocus(view);
    } else {
      setDagView(view);
    }
  };

  useShortcut({
    ...SHORTCUTS.dagView.toggleGraphGrid,
    callback: () => {
      const newView = dagView === "graph" ? "grid" : "graph";

      setDagView(newView);
      handleFocus(newView);
    },
    dependencies: [dagView],
    options: { preventDefault: true },
  });

  return (
    <Box position="relative" width="100%" zIndex={1}>
      <Flex justifyContent="space-between">
        <ButtonGroupToggle
          bg="bg"
          borderRadius="md"
          isIcon
          onChange={handleDagViewChange}
          options={dagViewOptions}
          value={dagView}
        />
        <Flex alignItems="center" gap={1} justifyContent="space-between">
          {dagView !== "graph" && <RunTypeLegend />}
          <ToggleGroups bg="bg" borderRadius="md" />
          {dagView === "graph" && <GraphTaskFilters />}
          <TaskStreamFilter />
          {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
          <Popover.Root autoFocus={false} positioning={{ placement: "bottom-end" }}>
            <OptionsTrigger label={translate("dag:panel.buttons.options")} />
            <Portal>
              <Popover.Positioner>
                <Popover.Content>
                  <Popover.Body
                    display="flex"
                    flexDirection="column"
                    gap={4}
                    maxH="70vh"
                    overflowY="auto"
                    p={2}
                  >
                    {dagView === "graph" ? (
                      <>
                        <DagVersionSelect />
                        <DagRunSelect limit={limit} />

                        <Switch
                          checked={showAllDependencies}
                          data-testid="show-all-dependencies"
                          onCheckedChange={(details) => setShowAllDependencies(details.checked)}
                        >
                          {translate("dag:panel.dependencies.allDagDependencies")}
                        </Switch>

                        <DirectionDropdown graphId={dagId} />
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
                        <VStack alignItems="flex-start" px={1}>
                          <VersionIndicatorSelect
                            onChange={setShowVersionIndicatorMode}
                            value={showVersionIndicatorMode}
                          />
                        </VStack>
                      </>
                    )}
                  </Popover.Body>
                </Popover.Content>
              </Popover.Positioner>
            </Portal>
          </Popover.Root>
        </Flex>
      </Flex>

      {dagView !== "graph" && (
        <Flex justifyContent="space-between" mt={2}>
          <GridFilters />
        </Flex>
      )}
    </Box>
  );
};
