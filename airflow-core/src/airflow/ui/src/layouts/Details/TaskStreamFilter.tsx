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
  Portal,
  Separator,
  Text,
  VStack,
} from "@chakra-ui/react";
import { useEffect } from "react";
import { useTranslation } from "react-i18next";
import { FiFilter, FiInfo } from "react-icons/fi";
import { useParams, useSearchParams } from "react-router-dom";

import { Tooltip } from "src/components/ui";
import { Menu } from "src/components/ui/Menu";

export const TaskStreamFilter = () => {
  const { t: translate } = useTranslation(["common", "components", "dag"]);
  const { taskId: currentTaskId } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const filterRoot = searchParams.get("root") ?? undefined;
  const includeUpstream = searchParams.get("upstream") === "true";
  const includeDownstream = searchParams.get("downstream") === "true";
  const depth = searchParams.get("depth") ?? undefined;
  const mode = searchParams.get("mode") ?? "static";

  const hasActiveFilter = includeUpstream || includeDownstream;
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

            {hasActiveFilter && filterRoot !== undefined ? (
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
          </Menu.Content>
        </Menu.Positioner>
      </Portal>
    </Menu.Root>
  );
};
