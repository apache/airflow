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
import { Portal, Popover, Text, VStack } from "@chakra-ui/react";
import { FiChevronDown, FiFilter } from "react-icons/fi";

import { Button } from "src/components/ui";

export type FilterMode = "both" | "downstream" | "none" | "upstream";

type LineageFilterProps = {
  readonly currentTaskId?: string; // The currently selected/hovered task from URL
  readonly filterMode: FilterMode;
  readonly filterRoot: string | undefined;
  readonly onClearFilter: () => void;
  readonly onFilterModeChange: (mode: FilterMode) => void;
  readonly translate: (key: string) => string;
};

export const LineageFilter = ({
  currentTaskId,
  filterMode,
  filterRoot,
  onClearFilter,
  onFilterModeChange,
  translate,
}: LineageFilterProps) => {
  const isCurrentTaskTheRoot = currentTaskId === filterRoot;
  const activeMode = isCurrentTaskTheRoot ? filterMode : "none";

  const getButtonLabel = () => {
    // Show "Lineage" when no task is selected or no filter is active
    if (filterRoot === undefined || filterMode === "none") {
      return translate("dag:panel.lineage.label");
    }

    // Show "task_name: filter_mode" when a filter is active
    const modeLabels = {
      both: translate("dag:panel.lineage.options.both"),
      downstream: translate("dag:panel.lineage.options.downstream"),
      none: "",
      upstream: translate("dag:panel.lineage.options.upstream"),
    };

    return `${filterRoot}: ${modeLabels[filterMode]}`;
  };

  return (
    <Popover.Root positioning={{ placement: "bottom-end" }}>
      <Popover.Trigger asChild>
        <Button bg="bg.subtle" color="fg.default" size="sm" variant="outline">
          <FiFilter />
          {getButtonLabel()}
          <FiChevronDown size={8} />
        </Button>
      </Popover.Trigger>
      <Portal>
        <Popover.Positioner>
          <Popover.Content>
            <Popover.Arrow />
            <Popover.Body display="flex" flexDirection="column" gap={2} p={3}>
              <VStack align="start" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">
                  {translate("dag:panel.lineage.label")}
                </Text>

                {filterRoot !== undefined && filterMode !== "none" ? (
                  <Text color="blue.600" fontSize="xs" fontWeight="medium">
                    {translate("dag:panel.lineage.activeFilter")}: {filterRoot} -{" "}
                    {filterMode === "upstream"
                      ? translate("dag:panel.lineage.options.upstream")
                      : filterMode === "downstream"
                        ? translate("dag:panel.lineage.options.downstream")
                        : translate("dag:panel.lineage.options.both")}
                  </Text>
                ) : undefined}

                {currentTaskId === undefined ? undefined : (
                  <Text color="fg.muted" fontSize="xs">
                    {translate("dag:panel.lineage.selectedTask")}: <strong>{currentTaskId}</strong>
                  </Text>
                )}

                {currentTaskId === undefined ? (
                  <Text color="fg.muted" fontSize="xs">
                    {translate("dag:panel.lineage.clickTask")}
                  </Text>
                ) : undefined}

                <VStack align="stretch" gap={1} width="100%">
                  <Button
                    colorPalette={activeMode === "upstream" ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={() => onFilterModeChange("upstream")}
                    size="sm"
                    variant={activeMode === "upstream" ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.lineage.options.upstream")}
                  </Button>

                  <Button
                    colorPalette={activeMode === "downstream" ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={() => onFilterModeChange("downstream")}
                    size="sm"
                    variant={activeMode === "downstream" ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.lineage.options.downstream")}
                  </Button>

                  <Button
                    colorPalette={activeMode === "both" ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={() => onFilterModeChange("both")}
                    size="sm"
                    variant={activeMode === "both" ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.lineage.options.both")}
                  </Button>
                </VStack>

                {filterMode !== "none" && filterRoot !== undefined ? (
                  <Button
                    colorPalette="red"
                    onClick={() => {
                      onClearFilter();
                    }}
                    size="sm"
                    variant="outline"
                    width="100%"
                  >
                    {translate("dag:panel.lineage.clearFilter")}
                  </Button>
                ) : undefined}
              </VStack>
            </Popover.Body>
          </Popover.Content>
        </Popover.Positioner>
      </Portal>
    </Popover.Root>
  );
};
