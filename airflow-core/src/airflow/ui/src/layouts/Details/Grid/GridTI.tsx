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
import { Badge, chakra, Flex } from "@chakra-ui/react";
import type { MouseEvent } from "react";
import React, { useRef } from "react";
import { useTranslation } from "react-i18next";
import { Link, useParams } from "react-router-dom";

import type { LightGridTaskInstanceSummary } from "openapi/requests/types.gen";
import { StateIcon } from "src/components/StateIcon";
import Time from "src/components/Time";

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
    task.style.backgroundColor = "var(--chakra-colors-blue-subtle)";
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
  const debounceTimeoutRef = useRef<NodeJS.Timeout | undefined>(undefined);
  const tooltipRef = useRef<HTMLElement | undefined>(undefined);

  const onBadgeMouseEnter = (event: MouseEvent<HTMLDivElement>) => {
    // Clear any existing timeout
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }

    // Store reference to the tooltip element
    const tooltip = event.currentTarget.querySelector("#tooltip") as HTMLElement;

    tooltipRef.current = tooltip;

    // Set a new timeout to show the tooltip after 200ms
    debounceTimeoutRef.current = setTimeout(() => {
      if (tooltipRef.current) {
        tooltipRef.current.style.visibility = "visible";
      }
    }, 200);
  };

  const onBadgeMouseLeave = () => {
    // Clear any existing timeout
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
      debounceTimeoutRef.current = undefined;
    }

    // Hide the tooltip immediately
    if (tooltipRef.current) {
      tooltipRef.current.style.visibility = "hidden";
      tooltipRef.current = undefined;
    }
  };

  return (
    <Flex
      alignItems="center"
      bg={selectedTaskId === taskId || selectedGroupId === taskId ? "blue.muted" : undefined}
      height="20px"
      id={taskId.replaceAll(".", "-")}
      justifyContent="center"
      key={taskId}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      px="2px"
      py={0}
      transition="background-color 0.2s"
      zIndex={1}
    >
      <Link
        onClick={onClick}
        replace
        to={{
          pathname: `/dags/${dagId}/runs/${runId}/tasks/${isGroup ? "group/" : ""}${taskId}${isMapped ? "/mapped" : ""}`,
          search,
        }}
      >
        <Badge
          borderRadius={4}
          colorPalette={instance.state ?? "none"}
          height="14px"
          minH={0}
          onMouseEnter={onBadgeMouseEnter}
          onMouseLeave={onBadgeMouseLeave}
          p={0}
          position="relative"
          variant="solid"
          width="14px"
        >
          <StateIcon
            size={10}
            state={instance.state}
            style={{
              marginLeft: "2px",
            }}
          />
          <chakra.span
            bg="bg.inverted"
            borderRadius={2}
            bottom={0}
            color="fg.inverted"
            id="tooltip"
            p={2}
            position="absolute"
            right={5}
            visibility="hidden"
            zIndex="tooltip"
          >
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
            {/* Tooltip arrow pointing to the badge */}
            <chakra.div
              bg="bg.inverted"
              borderRadius={1}
              bottom={1}
              height={2}
              position="absolute"
              right="-3px"
              transform="rotate(45deg)"
              width={2}
            />
          </chakra.span>
        </Badge>
      </Link>
    </Flex>
  );
};

export const GridTI = React.memo(Instance);
