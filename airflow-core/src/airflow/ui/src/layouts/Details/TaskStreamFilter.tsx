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
import { Button, Input, Portal, Separator, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiFilter } from "react-icons/fi";
import { useParams, useSearchParams } from "react-router-dom";

import { Menu } from "src/components/ui/Menu";

export const TaskStreamFilter = () => {
  const { t: translate } = useTranslation(["components", "dag"]);
  const { taskId: currentTaskId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";
  const depth = searchParams.get("depth") ?? undefined;

  const isCurrentTaskTheRoot = currentTaskId === filterRoot;
  const bothActive = isCurrentTaskTheRoot && includeUpstream && includeDownstream;
  const activeUpstream = isCurrentTaskTheRoot && includeUpstream && !includeDownstream;
  const activeDownstream = isCurrentTaskTheRoot && includeDownstream && !includeUpstream;
  const hasActiveFilter = includeUpstream || includeDownstream;

  const buildFilterSearch = (upstream: boolean, downstream: boolean, root?: string, newDepth?: string) => {
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

    if (root !== undefined && root !== "" && (upstream || downstream)) {
      searchParams.set("root", root);
    } else {
      searchParams.delete("root");
    }

    if (newDepth !== undefined && newDepth !== "" && (upstream || downstream)) {
      searchParams.set("depth", newDepth);
    } else {
      searchParams.delete("depth");
    }

    setSearchParams(searchParams);
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
                <Menu.Item asChild value="upstream">
                  <Button
                    color={activeUpstream ? "white" : undefined}
                    colorPalette={activeUpstream ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={() => buildFilterSearch(true, false, currentTaskId, depth)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        buildFilterSearch(true, false, currentTaskId, depth);
                      }
                    }}
                    size="sm"
                    variant={activeUpstream ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.taskStreamFilter.options.upstream")}
                  </Button>
                </Menu.Item>

                <Menu.Item asChild value="downstream">
                  <Button
                    color={activeDownstream ? "white" : undefined}
                    colorPalette={activeDownstream ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={() => buildFilterSearch(false, true, currentTaskId, depth)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        buildFilterSearch(false, true, currentTaskId, depth);
                      }
                    }}
                    size="sm"
                    variant={activeDownstream ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.taskStreamFilter.options.downstream")}
                  </Button>
                </Menu.Item>

                <Menu.Item asChild value="both">
                  <Button
                    color={bothActive ? "white" : undefined}
                    colorPalette={bothActive ? "blue" : "gray"}
                    disabled={currentTaskId === undefined}
                    justifyContent="flex-start"
                    onClick={() => buildFilterSearch(true, true, currentTaskId, depth)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        buildFilterSearch(true, true, currentTaskId, depth);
                      }
                    }}
                    size="sm"
                    variant={bothActive ? "solid" : "ghost"}
                    width="100%"
                  >
                    {translate("dag:panel.taskStreamFilter.options.both")}
                  </Button>
                </Menu.Item>
              </VStack>
            </VStack>

            <Separator my={2} />

            {/* Depth Section */}
            <VStack align="stretch" gap={2} width="100%">
              <Text fontSize="xs" fontWeight="semibold">
                {translate("dag:panel.taskStreamFilter.depth")}
              </Text>
              <Input
                disabled={currentTaskId === undefined}
                min={0}
                onChange={(e) => {
                  const value = e.target.value;
                  buildFilterSearch(includeUpstream, includeDownstream, filterRoot, value);
                }}
                onKeyDown={(e) => {
                  e.stopPropagation();
                }}
                placeholder="All"
                size="sm"
                type="number"
                value={depth ?? ""}
              />
            </VStack>

            <Separator my={2} />

            {hasActiveFilter && filterRoot !== undefined ? (
              <Menu.Item asChild value="clear">
                <Button
                  onClick={() => buildFilterSearch(false, false)}
                  size="sm"
                  variant="outline"
                  width="100%"
                >
                  {translate("dag:panel.taskStreamFilter.clearFilter")}
                </Button>
              </Menu.Item>
            ) : undefined}
          </Menu.Content>
        </Menu.Positioner>
      </Portal>
    </Menu.Root>
  );
};
