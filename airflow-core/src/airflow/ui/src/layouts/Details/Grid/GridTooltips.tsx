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
import { VStack, HStack, Text, Badge } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";

import type { DagRunState, TaskInstanceState } from "openapi/requests/types.gen";
import { RunTypeIcon } from "src/components/RunTypeIcon";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui";
import { getDuration } from "src/utils";

type GridTooltipsProps = {
  readonly children: ReactNode;
  readonly endDate?: string | null;
  readonly logicalDate?: string | null;
  readonly note?: string | null;
  readonly runId: string;
  readonly runType?: string;
  readonly startDate?: string | null;
  readonly state: DagRunState | TaskInstanceState | null | undefined;
};

export const GridTooltips = ({
  children,
  endDate,
  logicalDate,
  note,
  runId,
  runType,
  startDate,
}: GridTooltipsProps) => {
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

  return (
    <Tooltip
      content={tooltipContent}
      portalled
      positioning={{
        offset: { crossAxis: 5, mainAxis: 8 },
        placement: "bottom-start",
      }}
    >
      {children}
    </Tooltip>
  );
};
