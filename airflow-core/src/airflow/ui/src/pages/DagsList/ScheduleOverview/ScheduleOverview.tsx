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
import { Box, Spinner, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useScheduleOverviewServiceGetDagScheduleOverview } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";

import { ScheduleOverviewHeader, ScheduleOverviewRow } from "./ScheduleOverviewRow";

const LABEL_COLUMN_WIDTH_PX = 280;

export const ScheduleOverview = () => {
  const { t: translate } = useTranslation("dags");
  const [dagIdPattern, setDagIdPattern] = useState("");

  const { data, error, isLoading } = useScheduleOverviewServiceGetDagScheduleOverview({
    ...(dagIdPattern.length > 0 ? { dagIdPattern: `%${dagIdPattern}%` } : {}),
  });

  const entries = data?.entries ?? [];
  const totalEntries = data?.total_entries ?? 0;

  return (
    <Box data-testid="dag-schedule-overview-root" p={6}>
      <VStack alignItems="stretch" gap={4}>
        <ScheduleOverviewHeader labelColumnWidth={LABEL_COLUMN_WIDTH_PX} totalEntries={totalEntries} />
        <Box>
          <input
            aria-label={translate("scheduleOverview.filterPlaceholder")}
            data-testid="schedule-overview-filter"
            onChange={(event) => setDagIdPattern(event.target.value)}
            placeholder={translate("scheduleOverview.filterPlaceholder")}
            style={{
              background: "var(--chakra-colors-bg)",
              border: "1px solid var(--chakra-colors-border)",
              borderRadius: 4,
              color: "var(--chakra-colors-fg)",
              padding: "4px 8px",
              width: "320px",
            }}
            value={dagIdPattern}
          />
        </Box>
        <ErrorAlert error={error} />
        {isLoading ? (
          <Box alignItems="center" display="flex" justifyContent="center" minH="120px">
            <Spinner size="md" />
          </Box>
        ) : entries.length === 0 ? (
          <Box>
            <Text color="fg.muted">{translate("scheduleOverview.noDags")}</Text>
          </Box>
        ) : (
          <VStack
            alignItems="stretch"
            borderColor="border"
            borderRadius="md"
            borderWidth={1}
            data-testid="schedule-overview-list"
            gap={0}
          >
            {entries.map((entry) => (
              <ScheduleOverviewRow entry={entry} key={entry.dag_id} />
            ))}
          </VStack>
        )}
      </VStack>
    </Box>
  );
};
