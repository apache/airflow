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
import { Portal, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiFilter } from "react-icons/fi";

import { Button } from "src/components/ui";
import { Menu } from "src/components/ui/Menu";

type TaskStreamFilterProps = {
  readonly currentTaskId?: string;
  readonly filterRoot: string | undefined;
  readonly includeDownstream: boolean;
  readonly includeUpstream: boolean;
  readonly onIncludeDownstreamChange: (include: boolean) => void;
  readonly onIncludeUpstreamChange: (include: boolean) => void;
  readonly onFilterRootChange: (root: string | undefined) => void;
};

export const TaskStreamFilter = ({
  currentTaskId,
  filterRoot,
  includeDownstream,
  includeUpstream,
  onIncludeDownstreamChange,
  onIncludeUpstreamChange,
  onFilterRootChange,
}: TaskStreamFilterProps) => {
  const { t: translate } = useTranslation(["components", "dag"]);
  const isCurrentTaskTheRoot = currentTaskId === filterRoot;
  const bothActive = isCurrentTaskTheRoot && includeUpstream && includeDownstream;
  const activeUpstream = isCurrentTaskTheRoot && includeUpstream && !includeDownstream;
  const activeDownstream = isCurrentTaskTheRoot && includeDownstream && !includeUpstream;
  const hasActiveFilter = includeUpstream || includeDownstream;

  const handleUpstreamClick = () => {
      if (currentTaskId) {
        onFilterRootChange(currentTaskId);
      }
      onIncludeUpstreamChange(true);
      onIncludeDownstreamChange(false);
  };

  const handleDownstreamClick = () => {
    if (currentTaskId) {
      onFilterRootChange(currentTaskId);
    }
    onIncludeDownstreamChange(true);
    onIncludeUpstreamChange(false);
  };

  const handleBothClick = () => {
    if (currentTaskId) {
      onFilterRootChange(currentTaskId);
    }
    onIncludeUpstreamChange(true);
    onIncludeDownstreamChange(true);
  };

  const handleClearClick = () => {
    onIncludeUpstreamChange(false);
    onIncludeDownstreamChange(false);
  };

  return (
    <Menu.Root positioning={{ placement: "bottom-end" }}>
      <Menu.Trigger asChild>
        <Button bg="bg.subtle" size="sm" variant="outline">
          <FiFilter />
          {filterRoot === undefined || !hasActiveFilter
            ? translate("dag:panel.taskStreamFilter.label")
            : `${filterRoot}: ${
                includeUpstream && includeDownstream
                  ? translate("dag:panel.taskStreamFilter.options.both")
                  : includeUpstream
                    ? translate("dag:panel.taskStreamFilter.options.upstream")
                    : translate("dag:panel.taskStreamFilter.options.downstream")
              }`}
          <FiChevronDown size={8} />
        </Button>
      </Menu.Trigger>
      <Portal>
        <Menu.Positioner>
          <Menu.Content>
            <VStack align="start" gap={2} p={3}>
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

              <VStack align="stretch" gap={1} width="100%">
                <Menu.Item asChild value="upstream">
                  <Button
                    colorPalette={activeUpstream ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={handleUpstreamClick}
                    size="sm"
                    variant={activeUpstream ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.taskStreamFilter.options.upstream")}
                  </Button>
                </Menu.Item>

                <Menu.Item asChild value="downstream">
                  <Button
                    colorPalette={activeDownstream ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={handleDownstreamClick}
                    size="sm"
                    variant={activeDownstream ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.taskStreamFilter.options.downstream")}
                  </Button>
                </Menu.Item>

                <Menu.Item asChild value="both">
                  <Button
                    colorPalette={bothActive ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={handleBothClick}
                    size="sm"
                    variant={bothActive ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.taskStreamFilter.options.both")}
                  </Button>
                </Menu.Item>
              </VStack>

              {hasActiveFilter && filterRoot !== undefined ? (
                <Menu.Item asChild value="clear">
                  <Button onClick={handleClearClick} size="sm" variant="outline" width="100%">
                    {translate("dag:panel.taskStreamFilter.clearFilter")}
                  </Button>
                </Menu.Item>
              ) : undefined}
            </VStack>
          </Menu.Content>
        </Menu.Positioner>
      </Portal>
    </Menu.Root>
  );
};
