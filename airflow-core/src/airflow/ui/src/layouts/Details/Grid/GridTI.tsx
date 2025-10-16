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
import { Badge, Flex, Box, Text } from "@chakra-ui/react";
import type { MouseEvent } from "react";
import React, { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Link, useLocation, useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { type HoverContextType, useHover } from "src/context/hover";
import { getDuration, renderDuration } from "src/utils";
import { buildTaskInstanceUrl } from "src/utils/links";

const handleMouseEnter =
  (setHoveredTaskId: HoverContextType["setHoveredTaskId"]) => (event: MouseEvent<HTMLDivElement>) => {
    const tasks = document.querySelectorAll<HTMLDivElement>(`#${event.currentTarget.id}`);

    tasks.forEach((taskEl) => {
      taskEl.style.backgroundColor = "var(--chakra-colors-info-subtle)";
    });
    setHoveredTaskId(event.currentTarget.id.replaceAll("-", "."));
  };

const handleMouseLeave = (taskId: string, setHoveredTaskId: HoverContextType["setHoveredTaskId"]) => () => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#${taskId.replaceAll(".", "-")}`);

  tasks.forEach((taskEl) => {
    taskEl.style.backgroundColor = "";
  });
  setHoveredTaskId(undefined);
};

type Props = {
  readonly dagId: string;
  readonly instance: LightGridTaskInstanceSummary;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean | null;
  readonly label: string;
  readonly onClick?: () => void;
  readonly runId: string;
  readonly search: string;
  readonly taskId: string;
};

const Instance = ({ dagId, instance, isGroup, isMapped, onClick, runId, search, taskId }: Props) => {
  const { setHoveredTaskId } = useHover();
  const { groupId: selectedGroupId, taskId: selectedTaskId } = useParams();
  const location = useLocation();
  const { t: translate } = useTranslation("common");

  const onMouseEnter = handleMouseEnter(setHoveredTaskId);
  const onMouseLeave = handleMouseLeave(taskId, setHoveredTaskId);

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

  const start: string | undefined = instance?.min_start_date
  const end: string | undefined = instance?.max_end_date
  const hasStart = start !== undefined;
  const hasEnd = end !== undefined;

  const serverDurationUnknown = (instance as unknown as { duration?: unknown }).duration;
  const serverDurationSeconds = typeof serverDurationUnknown === "number" ? serverDurationUnknown : undefined;

  const durationText =
    serverDurationSeconds === undefined ? getDuration(start, end) : renderDuration(serverDurationSeconds);

  const isSelected =
    ((selectedTaskId ?? undefined) !== undefined && selectedTaskId === taskId) ||
    ((selectedGroupId ?? undefined) !== undefined && selectedGroupId === taskId);

  return (
    <Flex
      alignItems="center"
      bg={isSelected ? "info.muted" : undefined}
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
        <Tooltip
          content={
            <Box>
              <Text>
                {translate("taskId")}: {taskId}
              </Text>
              <Text>
                {translate("state")}: {instance.state}
              </Text>

              {hasStart ? (
                <Text>
                  {translate("startDate")}: <Time datetime={start} />
                </Text>
              ) : undefined}

              {hasEnd ? (
                <Text>
                  {translate("endDate")}: <Time datetime={end} />
                </Text>
              ) : undefined}

              {durationText === undefined ? undefined : (
                <Text>
                  {translate("duration")}: {durationText}
                </Text>
              )}
            </Box>
          }
          portalled
          positioning={{ offset: { crossAxis: 5, mainAxis: 5 }, placement: "bottom-start" }}
        >
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
        </Tooltip>
      </Link>
    </Flex>
  );
};

export const GridTI = React.memo(Instance);
