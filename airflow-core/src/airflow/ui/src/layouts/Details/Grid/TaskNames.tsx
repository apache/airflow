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
import type { MouseEvent } from "react";
import { useTranslation } from "react-i18next";
import { FiChevronUp } from "react-icons/fi";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { TaskName } from "src/components/TaskName";
import { useOpenGroups } from "src/context/openGroups";

import type { GridTask } from "./utils";

type Props = {
  depth?: number;
  nodes: Array<GridTask>;
  onRowClick?: () => void;
};

const onMouseEnter = (event: MouseEvent<HTMLDivElement>) => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#${event.currentTarget.id}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "var(--chakra-colors-blue-subtle)";
  });
};

const onMouseLeave = (event: MouseEvent<HTMLDivElement>) => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#${event.currentTarget.id}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "";
  });
};

export const TaskNames = ({ nodes, onRowClick }: Props) => {
  const { t: translate } = useTranslation("dag");
  const { toggleGroupId } = useOpenGroups();
  const { dagId = "", groupId, taskId } = useParams();
  const [searchParams] = useSearchParams();

  return nodes.map((node) => (
    <Box
      bg={node.id === taskId || node.id === groupId ? "blue.muted" : undefined}
      borderBottomWidth={1}
      borderColor={node.isGroup ? "border.emphasized" : "border.muted"}
      cursor="pointer"
      id={node.id.replaceAll(".", "-")}
      key={node.id}
      maxHeight="20px"
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
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
              search: searchParams.toString(),
            }}
          >
            <Flex alignItems="center" width="100%">
              <TaskName
                fontSize="sm"
                fontWeight="normal"
                isGroup={true}
                isMapped={Boolean(node.is_mapped)}
                label={node.label}
                paddingLeft={node.depth * 3 + 2}
                setupTeardownType={node.setup_teardown_type}
              />
              <chakra.span
                _focus={{ outline: "none" }}
                alignItems="center"
                aria-label={translate("grid.buttons.toggleGroup")}
                cursor="pointer"
                display="inline-flex"
                ml={1}
                onClick={(event) => {
                  event.preventDefault();
                  event.stopPropagation();
                  toggleGroupId(node.id);
                }}
                px={1}
              >
                <FiChevronUp
                  size="1rem"
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
              search: searchParams.toString(),
            }}
          >
            <TaskName
              fontSize="sm"
              fontWeight="normal"
              isMapped={Boolean(node.is_mapped)}
              label={node.label}
              paddingLeft={node.depth * 3 + 2}
              setupTeardownType={node.setup_teardown_type}
            />
          </RouterLink>
        </Link>
      )}
    </Box>
  ));
};
