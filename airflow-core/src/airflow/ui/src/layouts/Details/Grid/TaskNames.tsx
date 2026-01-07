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
import { Box, chakra, Flex, Link } from "@chakra-ui/react";
import type { VirtualItem } from "@tanstack/react-virtual";
import type { MouseEvent } from "react";
import { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronUp } from "react-icons/fi";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { TaskName } from "src/components/TaskName";
import { useHover } from "src/context/hover";
import { useOpenGroups } from "src/context/openGroups";

import type { GridTask } from "./utils";

type Props = {
  readonly depth?: number;
  readonly nodes: Array<GridTask>;
  readonly onRowClick?: () => void;
  readonly virtualItems?: Array<VirtualItem>;
};

const ROW_HEIGHT = 20;

const indent = (depth: number) => `${depth * 0.75 + 0.5}rem`;

export const TaskNames = ({ nodes, onRowClick, virtualItems }: Props) => {
  const { t: translate } = useTranslation("dag");
  const { hoveredTaskId, setHoveredTaskId } = useHover();
  const { toggleGroupId } = useOpenGroups();
  const { dagId = "", groupId, taskId } = useParams();
  const [searchParams] = useSearchParams();

  const handleMouseEnter = useCallback(
    (event: MouseEvent<HTMLDivElement>) => {
      const { nodeId } = event.currentTarget.dataset;

      if (nodeId !== undefined) {
        setHoveredTaskId(nodeId);
      }
    },
    [setHoveredTaskId],
  );

  const handleMouseLeave = useCallback(() => setHoveredTaskId(undefined), [setHoveredTaskId]);

  const handleToggleGroup = useCallback(
    (event: MouseEvent<HTMLSpanElement>) => {
      event.preventDefault();
      event.stopPropagation();
      const groupNodeId = event.currentTarget.dataset.groupId;

      if (groupNodeId !== undefined) {
        toggleGroupId(groupNodeId);
      }
    },
    [toggleGroupId],
  );

  const search = searchParams.toString();

  // If virtualItems is provided, use virtualization; otherwise render all ite
  const itemsToRender =
    virtualItems ?? nodes.map((_, index) => ({ index, size: ROW_HEIGHT, start: index * ROW_HEIGHT }));

  return (
    <>
      {itemsToRender.map((virtualItem) => {
        const node = nodes[virtualItem.index];

        if (!node) {
          return undefined;
        }

        const isSelected = node.id === taskId || node.id === groupId;
        const isHovered = hoveredTaskId === node.id;

        return (
          <Box
            bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
            borderBottomWidth={1}
            borderColor={node.isGroup ? "border.emphasized" : "border"}
            borderTopWidth={virtualItem.index === 0 ? 1 : 0}
            cursor="pointer"
            data-node-id={node.id}
            height={`${ROW_HEIGHT}px`}
            id={`task-${node.id.replaceAll(".", "-")}`}
            key={node.id}
            left={0}
            onMouseEnter={handleMouseEnter}
            onMouseLeave={handleMouseLeave}
            position="absolute"
            right={0}
            top={0}
            transform={`translateY(${virtualItem.start}px)`}
            transition="background-color 0.2s"
          >
            {node.isGroup ? (
              <Link asChild data-testid={node.id} display="block" width="100%">
                <RouterLink
                  onClick={onRowClick}
                  replace
                  style={{ outline: "none" }}
                  to={{
                    pathname: `/dags/${dagId}/tasks/group/${node.id}`,
                    search,
                  }}
                >
                  <Flex alignItems="center" width="100%">
                    <TaskName
                      fontSize="sm"
                      fontWeight="normal"
                      isGroup={true}
                      isMapped={Boolean(node.is_mapped)}
                      label={node.label}
                      paddingLeft={indent(node.depth)}
                      setupTeardownType={node.setup_teardown_type}
                    />
                    <chakra.span
                      _focus={{ outline: "none" }}
                      alignItems="center"
                      aria-label={translate("grid.buttons.toggleGroup")}
                      cursor="pointer"
                      data-group-id={node.id}
                      display="inline-flex"
                      ml={1}
                      onClick={handleToggleGroup}
                      px={1}
                    >
                      <FiChevronUp
                        size={16}
                        style={{
                          transform: `rotate(${node.isOpen ? 0 : 180}deg)`,
                          transition: "transform 0.5s",
                        }}
                      />
                    </chakra.span>
                  </Flex>
                </RouterLink>
              </Link>
            ) : (
              <Link asChild data-testid={node.id} display="inline">
                <RouterLink
                  onClick={onRowClick}
                  replace
                  to={{
                    pathname: `/dags/${dagId}/tasks/${node.id}`,
                    search,
                  }}
                >
                  <TaskName
                    fontSize="sm"
                    fontWeight="normal"
                    isMapped={Boolean(node.is_mapped)}
                    label={node.label}
                    paddingLeft={indent(node.depth)}
                    setupTeardownType={node.setup_teardown_type}
                  />
                </RouterLink>
              </Link>
            )}
          </Box>
        );
      })}
    </>
  );
};
