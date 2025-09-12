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
import React, { useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Link, useLocation, useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { buildTaskInstanceUrl } from "src/utils/links";

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

const onMouseEnter = (event: MouseEvent<HTMLDivElement>) => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#${event.currentTarget.id}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "var(--chakra-colors-brand-subtle)";
  });
};

const onMouseLeave = (event: MouseEvent<HTMLDivElement>) => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#${event.currentTarget.id}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "";
  });
};

const Instance = ({ dagId, instance, isGroup, isMapped, onClick, runId, search, taskId }: Props) => {
  const { groupId: selectedGroupId, taskId: selectedTaskId } = useParams();
  const { t: translate } = useTranslation();
  const location = useLocation();

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
      bg={selectedTaskId === taskId || selectedGroupId === taskId ? "brand.muted" : undefined}
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
            <>
              {translate("taskId")}: {taskId}
              <br />
              {translate("state")}: {instance.state}
              {instance.min_start_date !== null && (
                <>
                  <br />
                  {translate("startDate")}: <Time datetime={instance.min_start_date} />
                </>
              )}
              {instance.max_end_date !== null && (
                <>
                  <br />
                  {translate("endDate")}: <Time datetime={instance.max_end_date} />
                </>
              )}
            </>
          }
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
