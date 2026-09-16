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
import { Badge, Box, Flex } from "@chakra-ui/react";
import { Link, useLocation, useParams, useSearchParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { StateIcon } from "src/components/StateIcon";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";

import { useColorMode } from "src/context/colorMode";
import { buildTaskInstanceUrl } from "src/utils/links";

import { NOTE_GRADIENT, SELECTED_TASK_OUTLINE_COLOR } from "./constants";

type Props = {
  readonly dagId: string;
  readonly hasNote?: boolean;
  readonly instance: LightGridTaskInstanceSummary;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean | null;
  readonly label: string;
  readonly onClick?: () => void;
  readonly runId: string;
  readonly taskId: string;
};

export const GridTI = ({
  dagId,
  hasNote = false,
  instance,
  isGroup,
  isMapped,
  onClick,
  runId,
  taskId,
}: Props) => {
  const { groupId: selectedGroupId, runId: selectedRunId, taskId: selectedTaskId } = useParams();
  const { colorMode = "light" } = useColorMode();
  const location = useLocation();

  const [searchParams] = useSearchParams();

  const hasTaskInstance = instance.dag_version_number !== null && instance.dag_version_number !== undefined;
  const taskUrl = hasTaskInstance
    ? buildTaskInstanceUrl({
        currentPathname: location.pathname,
        dagId,
        isGroup,
        isMapped: Boolean(isMapped),
        runId,
        taskId,
      })
    : `/dags/${dagId}/tasks/${isGroup ? "group/" : ""}${taskId}`;

  // Remove try_number query param when navigating to reset to the
  // latest try of the task instance and avoid issues with invalid try numbers:
  // https://github.com/apache/airflow/issues/56977
  searchParams.delete("try_number");
  const redirectionSearch = searchParams.toString();

  const isSelectedRow = selectedTaskId === taskId || selectedGroupId === taskId;
  const isSelectedTaskInstance = selectedRunId === runId && isSelectedRow;
  const selectedOutlineColor = SELECTED_TASK_OUTLINE_COLOR[colorMode];

  return (
    <Flex
      alignItems="center"
      bg={isSelectedRow ? "brand.emphasized" : undefined}
      data-run-id={runId}
      data-selected={isSelectedRow}
      data-task-id={taskId}
      height="20px"
      id={`task-${taskId.replaceAll(".", "-")}`}
      justifyContent="center"
      key={taskId}
      position="relative"
      px="2px"
      py={0}
      transition="background-color 0.2s"
    >
      <TaskInstanceTooltip
        openDelay={500}
        positioning={{ placement: "bottom" }}
        runId={runId}
        taskInstance={instance}
      >
        <Box as="span" display="inline-block">
          <Link
            data-testid={`grid-${runId}-${taskId}`}
            id={`grid-${runId}-${taskId}`}
            onClick={onClick}
            replace
            to={{
              pathname: taskUrl,
              search: redirectionSearch,
            }}
          >
            <Badge
              alignItems="center"
              borderRadius={4}
              colorPalette={instance.state ?? "none"}
              data-selected={isSelectedTaskInstance || undefined}
              data-testid="task-state-badge"
              display="flex"
              height="14px"
              justifyContent="center"
              minH={0}
              outlineColor={isSelectedTaskInstance ? selectedOutlineColor : undefined}
              outlineOffset="1px"
              outlineStyle={isSelectedTaskInstance ? "solid" : undefined}
              outlineWidth={isSelectedTaskInstance ? "2px" : undefined}
              p={0}
              style={hasNote ? { background: NOTE_GRADIENT } : undefined}
              transition="outline-color 0.2s"
              variant="solid"
              width="14px"
            >
              <StateIcon size={10} state={instance.state} />
            </Badge>
          </Link>
        </Box>
      </TaskInstanceTooltip>
    </Flex>
  );
};
