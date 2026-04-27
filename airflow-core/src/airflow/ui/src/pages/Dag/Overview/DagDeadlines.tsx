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

import { useDeadlinesServiceGetDagDeadlineAlerts } from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { useDeadlines } from "src/queries/useDeadlines";
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
  const [modalOpen, setModalOpen] = useState<"missed" | "pending" | undefined>(undefined);

  const {
    data: pendingData,
    error: pendingError,
    isLoading: isPendingLoading,
  } = useDeadlines({
    dagId,
    endDate,
    limit: LIMIT,
    missed: false,
    refetchInterval,
    startDate,
  });

  const {
    data: missedData,
    error: missedError,
    isLoading: isMissedLoading,
  } = useDeadlines({
    dagId,
    endDate,
    limit: LIMIT,
    missed: true,
    refetchInterval,
    startDate,
  });

  const { data: alertData } = useDeadlinesServiceGetDagDeadlineAlerts({ dagId, limit: 100 });

  const alertMap = new Map<string, DeadlineAlertResponse>();

  for (const alert of alertData?.deadline_alerts ?? []) {
    alertMap.set(alert.id, alert);
  }

  const pendingDeadlines = pendingData?.deadlines ?? [];
  const missedDeadlines = missedData?.deadlines ?? [];

  if (
    !isPendingLoading &&
    !isMissedLoading &&
    pendingError === null &&
    missedError === null &&
    pendingDeadlines.length === 0 &&
    missedDeadlines.length === 0
  ) {
    return undefined;
  }

  const getAlert = (alertId?: string | null) =>
    alertId !== undefined && alertId !== null ? alertMap.get(alertId) : undefined;

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
                  <DeadlineRow alert={getAlert(dl.alert_id)} deadline={dl} key={dl.id} />
                ))}
                {(pendingData?.total_entries ?? 0) > LIMIT ? (
                  <Button
                    mt={2}
                    onClick={() => setModalOpen("pending")}
                    size="xs"
                    variant="ghost"
                    width="100%"
                  >
                    {translate("deadlineStatus.viewAll", {
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
                  <DeadlineRow alert={getAlert(dl.alert_id)} deadline={dl} key={dl.id} />
                ))}
                {(missedData?.total_entries ?? 0) > LIMIT ? (
                  <Button
                    mt={2}
                    onClick={() => setModalOpen("missed")}
                    size="xs"
                    variant="ghost"
                    width="100%"
                  >
                    {translate("deadlineStatus.viewAll", {
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
        alertMap={alertMap}
        dagId={dagId}
        endDate={endDate}
        missed={modalOpen === "missed"}
        onClose={() => setModalOpen(undefined)}
        open={modalOpen !== undefined}
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
