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
import { Badge, Box, Flex, HStack, Link, Stack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams } from "react-router-dom";

import { useAssetServiceGetAssetDeadlines } from "openapi/queries";

import { ProgressBar } from "src/system-components";

import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

const formatSeconds = (totalSeconds: number): string => {
  const abs = Math.abs(Math.floor(totalSeconds));
  const days = Math.floor(abs / 86_400);
  const hours = Math.floor((abs % 86_400) / 3600);
  const minutes = Math.floor((abs % 3600) / 60);
  const seconds = abs % 60;
  const parts: Array<string> = [];

  if (days > 0) {
    parts.push(`${days}d`);
  }
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0) {
    parts.push(`${minutes}m`);
  }
  if (seconds > 0) {
    parts.push(`${seconds}s`);
  }

  return parts.length > 0 ? parts.join(" ") : "0s";
};

const formatInterval = (interval: Record<string, unknown>): string => {
  if (typeof interval.__data__ === "number") {
    return formatSeconds(interval.__data__);
  }

  const parts: Array<string> = [];
  const units: Array<[string, string]> = [
    ["days", "d"],
    ["hours", "h"],
    ["minutes", "m"],
    ["seconds", "s"],
  ];

  for (const [key, suffix] of units) {
    const val = interval[key];

    if (typeof val === "number" && val !== 0) {
      parts.push(`${val}${suffix}`);
    }
  }

  return parts.length > 0 ? parts.join(" ") : JSON.stringify(interval);
};

const hasText = (value: string | null | undefined): value is string =>
  value !== null && value !== undefined && value !== "";

export const AssetDeadlines = () => {
  const { t: translate } = useTranslation(["assets", "common"]);
  const { assetId: rawAssetId } = useParams();
  const assetId = rawAssetId === undefined ? 0 : parseInt(rawAssetId, 10);

  const { data, error, isLoading } = useAssetServiceGetAssetDeadlines({ assetId }, undefined, {
    enabled: Boolean(rawAssetId),
  });

  const alerts = data?.alerts ?? [];
  const deadlines = data?.deadlines ?? [];

  return (
    <Box display="flex" flex={1} flexDirection="column" gap={4} minH={0} overflow="auto" pt={2} px={1}>
      <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
      <ErrorAlert error={error} />

      {!isLoading && alerts.length === 0 && deadlines.length === 0 ? (
        <Text color="fg.muted" fontSize="sm">
          {translate("assets:deadlines.emptyState")}
        </Text>
      ) : null}

      {alerts.length > 0 ? (
        <Stack gap={2}>
          <Text fontSize="sm" fontWeight="semibold">
            {translate("assets:deadlines.rulesHeading")}
          </Text>
          {alerts.map((alert) => (
            <Box borderColor="border.emphasized" borderRadius="md" borderWidth="1px" key={alert.id} p={3}>
              <Flex alignItems="center" gap={2} justifyContent="space-between" mb={1}>
                <HStack gap={2}>
                  {hasText(alert.dag_id) ? (
                    <Link asChild color="fg.info" fontWeight="medium">
                      <RouterLink to={`/dags/${alert.dag_id}`}>{alert.dag_id}</RouterLink>
                    </Link>
                  ) : (
                    <Text color="fg.muted" fontWeight="medium">
                      {translate("assets:deadlines.unknownDag")}
                    </Text>
                  )}
                  {hasText(alert.name) ? (
                    <Text color="fg.muted" fontSize="xs">
                      ({alert.name})
                    </Text>
                  ) : null}
                </HStack>
                <Badge
                  colorPalette={alert.is_producer ? "green" : "gray"}
                  size="sm"
                  variant={alert.is_producer ? "solid" : "subtle"}
                >
                  {alert.is_producer
                    ? translate("assets:deadlines.contractBadge")
                    : translate("assets:deadlines.consumerBadge")}
                </Badge>
              </Flex>
              <Text color="fg.muted" fontSize="xs">
                {hasText(alert.cron)
                  ? translate("assets:deadlines.cronLabel", { cron: alert.cron })
                  : alert.interval !== null && alert.interval !== undefined
                    ? translate("assets:deadlines.intervalLabel", {
                        interval: formatInterval(alert.interval),
                      })
                    : null}
              </Text>
            </Box>
          ))}
        </Stack>
      ) : null}

      {deadlines.length > 0 ? (
        <Stack gap={2}>
          <Text fontSize="sm" fontWeight="semibold">
            {translate("assets:deadlines.scheduledHeading")}
          </Text>
          {deadlines.map((deadline) => {
            const isResolved = deadline.missed && hasText(deadline.resolved_at);
            const isLate = Boolean(deadline.is_late) || (deadline.missed && !isResolved);

            return (
              <Box borderColor="border.muted" borderRadius="md" borderWidth="1px" key={deadline.id} p={3}>
                <Flex alignItems="center" gap={2} justifyContent="space-between">
                  <Stack gap={0.5}>
                    <HStack fontSize="xs" gap={1}>
                      <Text color="fg.muted">{translate("assets:deadlines.nextEvaluation")}:</Text>
                      <Time datetime={deadline.deadline_time} />
                    </HStack>
                    {isResolved && hasText(deadline.resolved_at) ? (
                      <HStack fontSize="xs" gap={1}>
                        <Text color="fg.muted">{translate("assets:deadlines.resolvedAt")}:</Text>
                        <Time datetime={deadline.resolved_at} />
                      </HStack>
                    ) : null}
                    {hasText(deadline.partition_key) ? (
                      <Text color="fg.muted" fontSize="xs">
                        {translate("assets:deadlines.partitionKey", { key: deadline.partition_key })}
                      </Text>
                    ) : null}
                  </Stack>
                  <Badge
                    colorPalette={isResolved ? "green" : isLate ? "red" : "blue"}
                    size="sm"
                    variant="solid"
                  >
                    {isResolved
                      ? translate("assets:deadlines.resolved")
                      : isLate
                        ? translate("assets:deadlines.late")
                        : translate("assets:deadlines.pending")}
                  </Badge>
                </Flex>
              </Box>
            );
          })}
        </Stack>
      ) : null}
    </Box>
  );
};
