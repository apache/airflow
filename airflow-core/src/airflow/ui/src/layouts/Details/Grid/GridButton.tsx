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
import { Flex, VStack, HStack, Text, Badge, type FlexProps } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";

import type { DagRunState, TaskInstanceState } from "openapi/requests/types.gen";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { getDuration } from "src/utils";

type Props = {
  readonly dagId: string;
  readonly endDate?: string | null;
  readonly isGroup?: boolean;
  readonly label: string;
  readonly logicalDate?: string | null;
  readonly note?: string | null;
  readonly runId: string;
  readonly runType?: string;
  readonly searchParams: string;
  readonly startDate?: string | null;
  readonly state: DagRunState | TaskInstanceState | null | undefined;
  readonly taskId?: string;
} & FlexProps;

export const GridButton = ({
  children,
  dagId,
  endDate,
  isGroup,
  label,
  logicalDate,
  note,
  runId,
  runType,
  searchParams,
  startDate,
  state,
  taskId,
  ...rest
}: Props) => {
  const { t: translate } = useTranslation("common");

  const tooltipContent = (
    <VStack align="stretch" gap={2} minW="200px" p={2}>
      <HStack justify="space-between">
        <Text color="white" fontSize="xs" fontWeight="semibold">
          {runId}
        </Text>
        {Boolean(runType) && (
          <Badge size="sm" variant="subtle">
            {Boolean(runType) && (
              <RunTypeIcon
                runType={runType as "asset_triggered" | "backfill" | "manual" | "scheduled"}
                size="12px"
              />
            )}
            {runType}
          </Badge>
        )}
      </HStack>
      <VStack align="stretch" gap={0.5}>
        {Boolean(logicalDate) && (
          <HStack fontSize="xs" justify="space-between">
            <Text color="gray.200">{translate("logicalDate")}:</Text>
            <Time datetime={logicalDate} />
          </HStack>
        )}

        {Boolean(startDate) && (
          <HStack fontSize="xs" justify="space-between">
            <Text color="gray.200">{translate("duration")}:</Text>
            <Text>{getDuration(startDate, endDate)}</Text>
          </HStack>
        )}
        {Boolean(note) && (
          <HStack fontSize="xs" justify="space-between">
            <Text color="gray.200">{translate("note.label")}:</Text>
            <Text overflow="hidden" textOverflow="ellipsis" whiteSpace="nowrap">
              {note}
            </Text>
          </HStack>
        )}
      </VStack>
    </VStack>
  );

  return isGroup ? (
    <Tooltip
      content={tooltipContent}
      portalled
      positioning={{
        offset: { crossAxis: 5, mainAxis: 8 },
        placement: "bottom-start",
      }}
    >
      <Flex
        background={`${state}.solid`}
        borderRadius={2}
        height="10px"
        minW="14px"
        pb="2px"
        px="2px"
        {...rest}
      >
        {children}
      </Flex>
    </Tooltip>
  ) : (
    <Tooltip
      content={tooltipContent}
      portalled
      positioning={{
        offset: { crossAxis: 5, mainAxis: 8 },
        placement: "bottom-start",
      }}
    >
      <Link
        replace
        to={{
          pathname: `/dags/${dagId}/runs/${runId}/${taskId === undefined ? "" : `tasks/${taskId}`}`,
          search: searchParams.toString(),
        }}
      >
        <Flex
          background={`${state}.solid`}
          borderRadius={2}
          height="10px"
          pb="2px"
          px="2px"
          width="14px"
          {...rest}
        >
          {children}
        </Flex>
      </Link>
    </Tooltip>
  );
};
