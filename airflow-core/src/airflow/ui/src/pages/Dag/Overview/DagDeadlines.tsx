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
import { Badge, Box, Button, Flex, Heading, HStack, Separator, Skeleton, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { useAutoRefresh } from "src/utils";

import { AllDeadlinesModal } from "./AllDeadlinesModal";
import { DeadlineRow } from "./DeadlineRow";

const LIMIT = 5;

type DagDeadlinesProps = {
  readonly dagId: string;
  readonly endDate: string;
  readonly startDate: string;
};

export const DagDeadlines = ({ dagId, endDate, startDate }: DagDeadlinesProps) => {
  const { t: translate } = useTranslation("dag");
  const refetchInterval = useAutoRefresh({ dagId });
  const [modalOpen, setModalOpen] = useState<"missed" | "pending" | null>(null);

  const {
    data: pendingData,
    error: pendingError,
    isLoading: isPendingLoading,
  } = useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId: "~",
      deadlineTimeGte: endDate,
      limit: LIMIT,
      missed: false,
      orderBy: ["deadline_time"],
    },
    undefined,
    { refetchInterval },
  );

  const {
    data: missedData,
    error: missedError,
    isLoading: isMissedLoading,
  } = useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId: "~",
      lastUpdatedAtGte: startDate,
      lastUpdatedAtLte: endDate,
      limit: LIMIT,
      missed: true,
      orderBy: ["-last_updated_at"],
    },
    undefined,
    { refetchInterval },
  );

  const pendingDeadlines = pendingData?.deadlines ?? [];
  const missedDeadlines = missedData?.deadlines ?? [];

  if (
    !isPendingLoading &&
    !isMissedLoading &&
    pendingDeadlines.length === 0 &&
    missedDeadlines.length === 0
  ) {
    return undefined;
  }

  return (
    <Box>
      <Flex color="fg.muted" mb={2}>
        <FiClock />
        <Heading ml={1} size="xs">
          {translate("overview.deadlines.title")}
        </Heading>
      </Flex>
      <ErrorAlert error={pendingError ?? missedError} />
      <Flex flexDirection={{ base: "column", md: "row" }} gap={{ base: 4, md: 8 }}>
        {isPendingLoading || pendingDeadlines.length > 0 ? (
          <Box borderRadius="lg" borderWidth={1} flex={1} overflow="hidden" p={3}>
            <HStack mb={2}>
              <FiClock />
              <Heading size="xs">{translate("overview.deadlines.pending")}</Heading>
              {pendingData ? (
                <Badge colorPalette="blue" size="sm" variant="solid">
                  {pendingData.total_entries}
                </Badge>
              ) : undefined}
            </HStack>
            {isPendingLoading ? (
              <VStack>
                {Array.from({ length: 3 }).map((_, idx) => (
                  // eslint-disable-next-line react/no-array-index-key
                  <Skeleton height="36px" key={idx} width="100%" />
                ))}
              </VStack>
            ) : (
              <VStack gap={0} separator={<Separator />}>
                {pendingDeadlines.map((dl) => (
                  <DeadlineRow deadline={dl} key={dl.id} />
                ))}
                {(pendingData?.total_entries ?? 0) > LIMIT ? (
                  <Button
                    mt={2}
                    onClick={() => setModalOpen("pending")}
                    size="xs"
                    variant="ghost"
                    width="100%"
                  >
                    {translate("overview.deadlines.viewAll", {
                      count: pendingData?.total_entries,
                    })}
                  </Button>
                ) : undefined}
              </VStack>
            )}
          </Box>
        ) : undefined}

        {isMissedLoading || missedDeadlines.length > 0 ? (
          <Box borderRadius="lg" borderWidth={1} flex={1} overflow="hidden" p={3}>
            <HStack color="fg.error" mb={2}>
              <FiAlertTriangle />
              <Heading size="xs">{translate("overview.deadlines.recentlyMissed")}</Heading>
              {missedData ? (
                <Badge colorPalette="failed" size="sm" variant="solid">
                  {missedData.total_entries}
                </Badge>
              ) : undefined}
            </HStack>
            {isMissedLoading ? (
              <VStack>
                {Array.from({ length: 3 }).map((_, idx) => (
                  // eslint-disable-next-line react/no-array-index-key
                  <Skeleton height="36px" key={idx} width="100%" />
                ))}
              </VStack>
            ) : (
              <VStack gap={0} separator={<Separator />}>
                {missedDeadlines.map((dl) => (
                  <DeadlineRow deadline={dl} key={dl.id} />
                ))}
                {(missedData?.total_entries ?? 0) > LIMIT ? (
                  <Button
                    mt={2}
                    onClick={() => setModalOpen("missed")}
                    size="xs"
                    variant="ghost"
                    width="100%"
                  >
                    {translate("overview.deadlines.viewAll", {
                      count: missedData?.total_entries,
                    })}
                  </Button>
                ) : undefined}
              </VStack>
            )}
          </Box>
        ) : undefined}
      </Flex>

      <AllDeadlinesModal
        dagId={dagId}
        endDate={endDate}
        missed={modalOpen === "missed"}
        onClose={() => setModalOpen(null)}
        open={modalOpen !== null}
        refetchInterval={refetchInterval}
        startDate={startDate}
        title={
          modalOpen === "missed"
            ? translate("overview.deadlines.recentlyMissed")
            : translate("overview.deadlines.pending")
        }
      />
    </Box>
  );
};
