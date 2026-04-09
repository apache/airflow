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
import { Badge, Heading, HStack, Separator, Skeleton, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiClock } from "react-icons/fi";

import { useDeadlinesServiceGetDeadlines } from "openapi/queries";
import type { DeadlineAlertResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";
import { Dialog } from "src/components/ui";
import { Pagination } from "src/components/ui/Pagination";

dayjs.extend(duration);
dayjs.extend(relativeTime);

const PAGE_LIMIT = 10;

type DeadlineStatusModalProps = {
  readonly alertMap: Map<string, DeadlineAlertResponse>;
  readonly dagId: string;
  readonly dagRunId: string;
  readonly onClose: () => void;
  readonly open: boolean;
  readonly runEndDate: string | undefined;
};

export const DeadlineStatusModal = ({
  alertMap,
  dagId,
  dagRunId,
  onClose,
  open,
  runEndDate,
}: DeadlineStatusModalProps) => {
  const { t: translate } = useTranslation("dag");
  const [page, setPage] = useState(1);
  const offset = (page - 1) * PAGE_LIMIT;

  const { data, error, isLoading } = useDeadlinesServiceGetDeadlines(
    {
      dagId,
      dagRunId,
      limit: PAGE_LIMIT,
      offset,
      orderBy: ["deadline_time"],
    },
    undefined,
    { enabled: open },
  );

  const deadlines = data?.deadlines ?? [];
  const totalEntries = data?.total_entries ?? 0;

  const onOpenChange = () => {
    setPage(1);
    onClose();
  };

  return (
    <Dialog.Root onOpenChange={onOpenChange} open={open} scrollBehavior="inside" size="lg">
      <Dialog.Content backdrop p={4}>
        <Dialog.Header>
          <Heading size="sm">{translate("deadlineStatus.label")}</Heading>
        </Dialog.Header>
        <Dialog.CloseTrigger />
        <Dialog.Body pb={2}>
          <ErrorAlert error={error} />
          {isLoading ? (
            <VStack>
              {Array.from({ length: PAGE_LIMIT }).map((_, idx) => (
                // eslint-disable-next-line react/no-array-index-key
                <Skeleton height="52px" key={idx} width="100%" />
              ))}
            </VStack>
          ) : (
            <VStack gap={0} separator={<Separator />}>
              {deadlines.map((dl) => {
                const alert =
                  dl.alert_name !== undefined && dl.alert_name !== null && dl.alert_name !== ""
                    ? alertMap.get(dl.alert_name)
                    : undefined;
                const deadlineTime = dayjs(dl.deadline_time);
                let contextLine: string | undefined;

                if (dl.missed) {
                  if (runEndDate === undefined) {
                    contextLine = translate("deadlineStatus.stillRunning");
                  } else {
                    const diff = dayjs(runEndDate).diff(deadlineTime);

                    contextLine =
                      diff >= 0
                        ? translate("deadlineStatus.finishedLate", {
                            duration: dayjs.duration(diff).humanize(),
                          })
                        : translate("deadlineStatus.finishedEarly", {
                            duration: dayjs.duration(-diff).humanize(),
                          });
                  }
                } else {
                  const remaining = deadlineTime.diff(dayjs());

                  if (remaining > 0) {
                    contextLine = translate("deadlineStatus.deadlineIn", {
                      duration: dayjs.duration(remaining).humanize(),
                    });
                  }
                }

                return (
                  <VStack alignItems="flex-start" gap={0.5} key={dl.id} px={2} py={1.5} width="100%">
                    <HStack justifyContent="space-between" width="100%">
                      <HStack gap={2}>
                        <Badge colorPalette={dl.missed ? "red" : "blue"} size="sm" variant="solid">
                          {dl.missed ? <FiAlertTriangle /> : <FiClock />}
                          {dl.missed
                            ? translate("deadlineStatus.missed")
                            : translate("deadlineStatus.upcoming")}
                        </Badge>
                        {dl.alert_name === undefined ||
                        dl.alert_name === null ||
                        dl.alert_name === "" ? undefined : (
                          <Text color="fg.muted" fontSize="xs">
                            {dl.alert_name}
                          </Text>
                        )}
                      </HStack>
                      <Time datetime={dl.deadline_time} fontSize="sm" />
                    </HStack>
                    {alert === undefined ? undefined : (
                      <Text color="fg.muted" fontSize="xs">
                        {translate("deadlineStatus.completionRule", {
                          interval: dayjs.duration(alert.interval, "seconds").humanize(),
                          reference: translate(`deadlineAlerts.referenceType.${alert.reference_type}`, {
                            defaultValue: alert.reference_type,
                          }),
                        })}
                      </Text>
                    )}
                    {contextLine === undefined ? undefined : (
                      <Text color={dl.missed ? "fg.error" : "fg.muted"} fontSize="xs">
                        {contextLine}
                      </Text>
                    )}
                  </VStack>
                );
              })}
            </VStack>
          )}
        </Dialog.Body>
        {totalEntries > PAGE_LIMIT ? (
          <Pagination.Root
            count={totalEntries}
            onPageChange={(event) => setPage(event.page)}
            p={3}
            page={page}
            pageSize={PAGE_LIMIT}
          >
            <HStack justify="center">
              <Pagination.PrevTrigger />
              <Pagination.Items />
              <Pagination.NextTrigger />
            </HStack>
          </Pagination.Root>
        ) : undefined}
      </Dialog.Content>
    </Dialog.Root>
  );
};
