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
import { Badge, Button, HStack, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiCheck, FiClock } from "react-icons/fi";

import { useDeadlinesServiceGetDagDeadlineAlerts, useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";
import { Tooltip } from "src/components/ui/Tooltip";
import { renderDuration } from "src/utils/datetimeUtils";

import { DeadlineStatusModal } from "./DeadlineStatusModal";

dayjs.extend(duration);
dayjs.extend(relativeTime);

type DeadlineStatusProps = {
  readonly dagId: string;
  readonly dagRunId: string;
  readonly endDate: string | null;
};

export const DeadlineStatus = ({ dagId, dagRunId, endDate }: DeadlineStatusProps) => {
  const { t: translate } = useTranslation("dag");
  const [isModalOpen, setIsModalOpen] = useState(false);

  const { data: deadlineData, isLoading: isLoadingDeadlines } = useDeadlinesServiceGetDeadlines({
    dagId,
    dagRunId,
    limit: 10,
    orderBy: ["deadline_time"],
  });

  // Used to detect whether the DAG has any deadline alerts at all, so we can show "Met" when there are alerts configured but no active deadline instances.
  const { data: alertData, isLoading: isLoadingAlerts } = useDeadlinesServiceGetDagDeadlineAlerts({
    dagId,
    limit: 100,
  });

  const alertMap = new Map<string, DeadlineAlertResponse>();

  for (const deadlineAlert of alertData?.deadline_alerts ?? []) {
    alertMap.set(deadlineAlert.id, deadlineAlert);
  }

  if (isLoadingDeadlines || isLoadingAlerts) {
    return undefined;
  }

  // Active instances for this run; hasAlerts = DAG has deadline alerts configured.
  const deadlines = deadlineData?.deadlines ?? [];
  const hasAlerts = (alertData?.total_entries ?? 0) > 0;

  // No deadline alerts configured on the DAG at all
  if (deadlines.length === 0 && !hasAlerts) {
    return undefined;
  }

  // Alerts are configured but no active deadline instances exist therefore all deadlines were met.
  // Show a tooltip listing each alert's completion rule.
  if (deadlines.length === 0 && hasAlerts) {
    const tooltipContent = (
      <VStack alignItems="flex-start" gap={0.5}>
        {(alertData?.deadline_alerts ?? []).map((deadlineAlert) => (
          <Text fontSize="xs" key={deadlineAlert.id}>
            {translate("deadlineAlerts.completionRule", {
              interval: dayjs.duration(deadlineAlert.interval, "seconds").humanize(),
              reference: translate(`deadlineAlerts.referenceType.${deadlineAlert.reference_type}`, {
                defaultValue: deadlineAlert.reference_type,
              }),
            })}
          </Text>
        ))}
      </VStack>
    );

    return (
      <Tooltip content={tooltipContent}>
        <Badge colorPalette="green" size="sm" variant="solid">
          <FiCheck />
          {translate("deadlineStatus.met")}
        </Badge>
      </Tooltip>
    );
  }

  const totalEntries = deadlineData?.total_entries ?? 0;
  const runEndDate = endDate ?? undefined;

  // When there are multiple deadline instances, collapse into a single compact badge to
  // avoid stretching the run header. Clicking opens the modal with full details.
  // Counts are derived from the loaded page (limit: 10); a single run virtually never has more
  // than that, but the modal shows the authoritative paginated list either way.
  if (totalEntries > 1) {
    const missedCount = deadlines.filter((entry) => entry.missed).length;
    const upcomingCount = deadlines.length - missedCount;
    const hasMissed = missedCount > 0;
    const hasUpcoming = upcomingCount > 0;

    let label: string;

    if (hasMissed && hasUpcoming) {
      label = translate("deadlineStatus.mixedCount", { missedCount, upcomingCount });
    } else if (hasMissed) {
      label = translate("deadlineStatus.missedCount", { count: missedCount });
    } else {
      label = translate("deadlineStatus.upcomingCount", { count: upcomingCount });
    }

    return (
      <>
        <Button onClick={() => setIsModalOpen(true)} p={0} size="sm" variant="plain">
          <Badge colorPalette={hasMissed ? "red" : "blue"} size="sm" variant="solid">
            {hasMissed ? <FiAlertTriangle /> : <FiClock />}
            {label}
          </Badge>
        </Button>
        <DeadlineStatusModal
          alertMap={alertMap}
          dagId={dagId}
          dagRunId={dagRunId}
          onClose={() => setIsModalOpen(false)}
          open={isModalOpen}
          runEndDate={runEndDate}
        />
      </>
    );
  }

  // Single deadline — show inline with Expected / Actual dates and precise duration.
  const [dl] = deadlines;

  if (dl === undefined) {
    return undefined;
  }

  const alert = dl.alert_id !== undefined && dl.alert_id !== null ? alertMap.get(dl.alert_id) : undefined;
  const deadlineTime = dayjs(dl.deadline_time);

  let actualDurationLabel: string | undefined;

  if (dl.missed && runEndDate !== undefined) {
    const diff = dayjs(runEndDate).diff(deadlineTime);
    const dur = renderDuration(Math.abs(diff) / 1000, false);

    if (dur !== undefined) {
      actualDurationLabel =
        diff >= 0
          ? translate("deadlineStatus.finishedLate", { duration: dur })
          : translate("deadlineStatus.finishedEarly", { duration: dur });
    }
  }

  return (
    <VStack alignItems="flex-start" gap={0.5}>
      <HStack gap={1}>
        <Badge colorPalette={dl.missed ? "red" : "blue"} size="sm" variant="solid">
          {dl.missed ? <FiAlertTriangle /> : <FiClock />}
          {translate(dl.missed ? "deadlineStatus.missed" : "deadlineStatus.upcoming")}
        </Badge>
        {Boolean(dl.alert_name) && (
          <Text color="fg.muted" fontSize="xs">
            ({dl.alert_name})
          </Text>
        )}
      </HStack>
      {alert === undefined ? undefined : (
        <Text color="fg.muted" fontSize="xs">
          {translate("deadlineAlerts.completionRule", {
            interval: dayjs.duration(alert.interval, "seconds").humanize(),
            reference: translate(`deadlineAlerts.referenceType.${alert.reference_type}`, {
              defaultValue: alert.reference_type,
            }),
          })}
        </Text>
      )}
      <HStack gap={1}>
        <Text color="fg.muted" fontSize="xs">
          {translate("deadlineStatus.expected")}:
        </Text>
        <Time datetime={dl.deadline_time} fontSize="xs" />
      </HStack>
      <HStack gap={1}>
        <Text color="fg.muted" fontSize="xs">
          {translate("deadlineStatus.actual")}:
        </Text>
        {runEndDate === undefined ? (
          <Text color="fg.muted" fontSize="xs">
            {translate("deadlineStatus.stillRunning")}
          </Text>
        ) : (
          <Time datetime={runEndDate} fontSize="xs" />
        )}
      </HStack>
      {actualDurationLabel === undefined ? undefined : (
        <Text color="fg.error" fontSize="xs" pl={1}>
          {actualDurationLabel}
        </Text>
      )}
    </VStack>
  );
};
