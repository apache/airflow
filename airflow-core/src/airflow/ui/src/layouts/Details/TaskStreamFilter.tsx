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
import { Button, Portal, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiChevronDown, FiFilter } from "react-icons/fi";
import { Link, useParams, useSearchParams } from "react-router-dom";

import { Menu } from "src/components/ui/Menu";

export const TaskStreamFilter = () => {
  const { t: translate } = useTranslation(["components", "dag"]);
  const { taskId: currentTaskId } = useParams();
  const [searchParams] = useSearchParams();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";

  const isCurrentTaskTheRoot = currentTaskId === filterRoot;
  const bothActive = isCurrentTaskTheRoot && includeUpstream && includeDownstream;
  const activeUpstream = isCurrentTaskTheRoot && includeUpstream && !includeDownstream;
  const activeDownstream = isCurrentTaskTheRoot && includeDownstream && !includeUpstream;
  const hasActiveFilter = includeUpstream || includeDownstream;

  const buildFilterSearch = (upstream: boolean, downstream: boolean, root?: string) => {
    const newParams = new URLSearchParams(searchParams);

    if (upstream) {
      newParams.set("upstream", "true");
    } else {
      newParams.delete("upstream");
    }

    if (downstream) {
      newParams.set("downstream", "true");
    } else {
      newParams.delete("downstream");
    }

    if (root !== undefined && root !== "" && (upstream || downstream)) {
      newParams.set("root", root);
    } else {
      newParams.delete("root");
    }

    return newParams.toString();
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

            <VStack align="stretch" gap={1} width="100%">
              <Menu.Item asChild value="upstream">
                <Button
                  asChild
                  color={activeUpstream ? "white" : undefined}
                  colorPalette={activeUpstream ? "blue" : "gray"}
                  disabled={currentTaskId === undefined}
                  size="sm"
                  variant={activeUpstream ? "solid" : "ghost"}
                  width="100%"
                >
                  <Link
                    replace
                    style={{
                      justifyContent: "flex-start",
                      pointerEvents: currentTaskId === undefined ? "none" : "auto",
                    }}
                    to={{ search: buildFilterSearch(true, false, currentTaskId) }}
                  >
                    {translate("dag:panel.taskStreamFilter.options.upstream")}
                  </Link>
                </Button>
              </Menu.Item>

              <Menu.Item asChild value="downstream">
                <Button
                  asChild
                  color={activeDownstream ? "white" : undefined}
                  colorPalette={activeDownstream ? "blue" : "gray"}
                  disabled={currentTaskId === undefined}
                  size="sm"
                  variant={activeDownstream ? "solid" : "ghost"}
                  width="100%"
                >
                  <Link
                    replace
                    style={{
                      justifyContent: "flex-start",
                      pointerEvents: currentTaskId === undefined ? "none" : "auto",
                    }}
                    to={{ search: buildFilterSearch(false, true, currentTaskId) }}
                  >
                    {translate("dag:panel.taskStreamFilter.options.downstream")}
                  </Link>
                </Button>
              </Menu.Item>

              <Menu.Item asChild value="both">
                <Button
                  asChild
                  color={bothActive ? "white" : undefined}
                  colorPalette={bothActive ? "blue" : "gray"}
                  disabled={currentTaskId === undefined}
                  size="sm"
                  variant={bothActive ? "solid" : "ghost"}
                  width="100%"
                >
                  <Link
                    replace
                    style={{
                      justifyContent: "flex-start",
                      pointerEvents: currentTaskId === undefined ? "none" : "auto",
                    }}
                    to={{ search: buildFilterSearch(true, true, currentTaskId) }}
                  >
                    {translate("dag:panel.taskStreamFilter.options.both")}
                  </Link>
                </Button>
              </Menu.Item>
            </VStack>

            {hasActiveFilter && filterRoot !== undefined ? (
              <Menu.Item asChild value="clear">
                <Button asChild size="sm" variant="outline" width="100%">
                  <Link
                    replace
                    style={{ justifyContent: "center" }}
                    to={{ search: buildFilterSearch(false, false) }}
                  >
                    {translate("dag:panel.taskStreamFilter.clearFilter")}
                  </Link>
                </Button>
              </Menu.Item>
            ) : undefined}
          </Menu.Content>
        </Menu.Positioner>
      </Portal>
    </Menu.Root>
  );
};
