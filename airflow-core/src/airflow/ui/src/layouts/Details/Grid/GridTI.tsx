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
import React, { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import { Link, useLocation, useParams, useSearchParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { BasicTooltip } from "src/components/BasicTooltip";
import { StateIcon } from "src/components/StateIcon";
import Time from "src/components/Time";
import { useHover } from "src/context/hover";
import { buildTaskInstanceUrl } from "src/utils/links";

type Props = {
  readonly dagId: string;
  readonly instance: LightGridTaskInstanceSummary;
  readonly isGroup?: boolean;
  readonly isMapped?: boolean | null;
  readonly label: string;
  readonly onClick?: () => void;
  readonly runId: string;
  readonly taskId: string;
};

const Instance = ({ dagId, instance, isGroup, isMapped, onClick, runId, taskId }: Props) => {
  const { hoveredTaskId, setHoveredTaskId } = useHover();
  const { groupId: selectedGroupId, taskId: selectedTaskId } = useParams();
  const { t: translate } = useTranslation();
  const location = useLocation();

  const [searchParams] = useSearchParams();

  const taskUrl = useMemo(
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

  const handleMouseEnter = useCallback(() => setHoveredTaskId(taskId), [setHoveredTaskId, taskId]);
  const handleMouseLeave = useCallback(() => setHoveredTaskId(undefined), [setHoveredTaskId]);

  // Remove try_number query param when navigating to reset to the
  // latest try of the task instance and avoid issues with invalid try numbers:
  // https://github.com/apache/airflow/issues/56977
  searchParams.delete("try_number");
  const redirectionSearch = searchParams.toString();

  // Determine background: selected takes priority over hovered
  const isSelected = selectedTaskId === taskId || selectedGroupId === taskId;
  const isHovered = hoveredTaskId === taskId;

  return (
    <Flex
      alignItems="center"
      bg={isSelected ? "brand.emphasized" : isHovered ? "brand.muted" : undefined}
      height="20px"
      id={`task-${taskId.replaceAll(".", "-")}`}
      justifyContent="center"
      key={taskId}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      position="relative"
      px="2px"
      py={0}
      transition="background-color 0.2s"
    >
      <BasicTooltip
        content={
          <>
            {translate("taskId")}: {taskId}
            <br />
            {translate("state")}:{" "}
            {instance.state
              ? translate(`common:states.${instance.state}`)
              : translate("common:states.no_status")}
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
        <Link
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
        </Link>
      </BasicTooltip>
    </Flex>
  );
};

export const GridTI = React.memo(Instance);
