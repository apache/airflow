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
import { Box, Button, HStack, Separator, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiClock } from "react-icons/fi";

import { useDeadlinesServiceGetDeadlines, useDeadlinesServiceGetDagDeadlineAlerts } from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import { Popover } from "src/components/ui";
import { renderDuration } from "src/utils";

const referenceTypeLabels: Record<string, string> = {
  AverageRuntimeDeadline: "Average Runtime",
  DagRunLogicalDateDeadline: "Logical Date",
  DagRunQueuedAtDeadline: "Queued At",
  FixedDatetimeDeadline: "Fixed Datetime",
};

const formatReferenceType = (referenceType: string): string =>
  referenceTypeLabels[referenceType] ?? referenceType;

const AlertRow = ({ alert }: { readonly alert: DeadlineAlertResponse }) => {
  const { t: translate } = useTranslation("dag");

  return (
    <Box py={2} width="100%">
      <Text fontWeight="bold">
        {alert.name !== undefined && alert.name !== null && alert.name !== ""
          ? alert.name
          : translate("deadlineAlerts.unnamed")}
      </Text>
      {alert.description !== undefined && alert.description !== null && alert.description !== "" ? (
        <Text color="fg.muted" fontSize="xs">
          {alert.description}
        </Text>
      ) : undefined}
      <HStack fontSize="xs" gap={3} mt={1}>
        <Text color="fg.muted">
          {translate("deadlineAlerts.referenceType")}: {formatReferenceType(alert.reference_type)}
        </Text>
        <Text color="fg.muted">
          {translate("deadlineAlerts.interval")}:{" "}
          {renderDuration(alert.interval, false) ?? `${alert.interval}s`}
        </Text>
      </HStack>
    </Box>
  );
};

export const DeadlineAlertsBadge = ({ dagId }: { readonly dagId: string }) => {
  const { t: translate } = useTranslation("dag");

  const { data } = useDeadlinesServiceGetDagDeadlineAlerts({ dagId });
  const { data: missedData } = useDeadlinesServiceGetDeadlines({
    dagId,
    dagRunId: "~",
    lastUpdatedAtGte: dayjs().subtract(24, "hour").toISOString(),
    limit: 1,
    missed: true,
  });

  const alerts = (data?.total_entries ?? 0) > 0 ? data?.deadline_alerts : [];
  const hasMissed = (missedData?.total_entries ?? 0) > 0;

  if (!alerts || alerts.length === 0) {
    return undefined;
  }

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount unmountOnExit>
      <Popover.Trigger asChild>
        <Button color={hasMissed ? "fg.error" : "fg.info"} size="xs" variant="outline">
          <FiClock />
          {translate("deadlineAlerts.count", { count: alerts.length })}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} maxWidth="360px" width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <Text fontWeight="bold" mb={1}>
            {translate("deadlineAlerts.title")}
          </Text>
          <VStack gap={0} separator={<Separator />}>
            {alerts.map((alert) => (
              <AlertRow alert={alert} key={alert.id} />
            ))}
          </VStack>
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
