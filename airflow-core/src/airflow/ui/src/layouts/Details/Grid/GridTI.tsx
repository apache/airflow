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
import { Badge, Flex } from "@chakra-ui/react";
import type { MouseEvent } from "react";
import React, { useCallback, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";

import { useTaskInstanceServiceGetTaskInstances } from "openapi/queries/queries.ts";
import type { LightGridTaskInstanceSummary, TaskInstanceResponse } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import TaskInstanceTooltip from "src/components/TaskInstanceTooltip";
import { type HoverContextType, useHover } from "src/context/hover";
import { buildTaskInstanceUrl } from "src/utils/links";

const handleMouseEnter =
  (setHoveredTaskId: HoverContextType["setHoveredTaskId"]) => (event: MouseEvent<HTMLDivElement>) => {
    const tasks = document.querySelectorAll<HTMLDivElement>(`#${event.currentTarget.id}`);

    tasks.forEach((task) => {
      task.style.backgroundColor = "var(--chakra-colors-info-subtle)";
    });
    setHoveredTaskId(event.currentTarget.id.replaceAll("-", "."));
  };

const handleMouseLeave = (taskId: string, setHoveredTaskId: HoverContextType["setHoveredTaskId"]) => () => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#${taskId.replaceAll(".", "-")}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "";
  });
  setHoveredTaskId(undefined);
};

type Props = {
  readonly dagId: string;
  readonly fullInstance?: TaskInstanceResponse; // optional richer data from parent
  readonly instance: LightGridTaskInstanceSummary;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean | null;
  readonly label: string;
  readonly onClick?: () => void;
  readonly runId: string;
  readonly search: string;
  readonly taskId: string;
};

const Instance = ({
  dagId,
  fullInstance,
  instance,
  isGroup,
  isMapped,
  onClick,
  runId,
  search,
  taskId,
}: Props) => {
  const { setHoveredTaskId } = useHover();
  const { groupId: selectedGroupId, taskId: selectedTaskId } = useParams();
  const location = useLocation();

  const [open, setOpen] = useState(false);

  const onMouseEnter = handleMouseEnter(setHoveredTaskId);
  const onMouseLeave = handleMouseLeave(taskId, setHoveredTaskId);

  // include the map index for mapped tasks so API returns the specific TI
  const mapIndexArray =
    "map_index" in instance && typeof instance.map_index === "number" ? [instance.map_index] : undefined;

  // Hydrate the tooltip with a full TaskInstance when opened (skip if parent already provided one)
  const { data: tiPage } = useTaskInstanceServiceGetTaskInstances(
    {
      dagId,
      dagRunId: runId,
      taskId,
      ...(mapIndexArray === undefined ? {} : { mapIndex: mapIndexArray }),
      limit: 1, // don't use orderBy here — it 400's this endpoint
    },
    undefined,
    {
      enabled: open && !fullInstance,
      staleTime: 15_000,
    },
  );

  // eslint fix: no unnecessary optional chain on array indexing
  const hydrated = fullInstance ?? tiPage?.task_instances[0] ?? instance;

  const getTaskUrl = useCallback(
    () =>
      buildTaskInstanceUrl({
        currentPathname: location.pathname,
        dagId,
        isGroup,
        isMapped: Boolean(isMapped),
        runId,
        taskId,
      }),
    [dagId, isGroup, isMapped, location.pathname, runId, taskId],
  );

  return (
    <Flex
      alignItems="center"
      bg={selectedTaskId === taskId || selectedGroupId === taskId ? "info.muted" : undefined}
      height="20px"
      id={taskId.replaceAll(".", "-")}
      justifyContent="center"
      key={taskId}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      position="relative"
      px="2px"
      py={0}
      transition="background-color 0.2s"
    >
      <Link
        id={`grid-${runId}-${taskId}`}
        onClick={onClick}
        replace
        to={{
          pathname: getTaskUrl(),
          search,
        }}
      >
        <TaskInstanceTooltip onOpenChange={(details) => setOpen(details.open)} taskInstance={hydrated}>
          <Badge
            alignItems="center"
            borderRadius={4}
            colorPalette={instance.state ?? "none"}
            display="flex"
            height="14px"
            justifyContent="center"
            minH={0}
            p={0}
            variant="solid"
            width="14px"
          >
            <StateIcon size={10} state={instance.state} />
          </Badge>
        </TaskInstanceTooltip>
      </Link>
    </Flex>
  );
};

export const GridTI = React.memo(Instance);
