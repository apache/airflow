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
import { HStack, Link, Text, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import relativeTime from "dayjs/plugin/relativeTime";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type {
  DAGRunResponse,
  DeadlineAlertResponse,
  DeadlineResponse,
  DagRunState,
} from "openapi/requests/types.gen";
import Time from "src/components/Time";

dayjs.extend(duration);
dayjs.extend(relativeTime);

type DeadlineRowProps = {
  readonly alert?: DeadlineAlertResponse;
  readonly deadline: DeadlineResponse;
  readonly run?: DAGRunResponse;
  readonly runState?: DagRunState;
};

export const DeadlineRow = ({ alert, deadline, run, runState }: DeadlineRowProps) => {
  const { t: translate } = useTranslation("dag");

  const reference = alert
    ? translate(`deadlineAlerts.referenceType.${alert.reference_type}`, {
        defaultValue: alert.reference_type,
      })
    : undefined;
  const interval = alert ? dayjs.duration(alert.interval, "seconds").humanize() : undefined;

  let contextLine: string | undefined;

  if (deadline.missed) {
    if (run?.end_date === undefined || run.end_date === null) {
      contextLine = translate("overview.deadlines.stillRunning");
    } else {
      const diff = dayjs(run.end_date).diff(dayjs(deadline.deadline_time));

      contextLine =
        diff >= 0
          ? translate("overview.deadlines.finishedLate", {
              duration: dayjs.duration(diff).humanize(),
            })
          : translate("overview.deadlines.finishedEarly", {
              duration: dayjs.duration(-diff).humanize(),
            });
    }
  }

  return (
    <HStack justifyContent="space-between" px={2} py={1.5} width="100%">
      <VStack alignItems="flex-start" gap={0}>
        <HStack gap={1}>
          <Link asChild color="fg.info" fontSize="sm" fontWeight="bold">
            <RouterLink to={`/dags/${deadline.dag_id}/runs/${deadline.dag_run_id}`}>
              {deadline.dag_run_id}
            </RouterLink>
          </Link>
          {deadline.missed && runState !== undefined ? (
            <Text fontSize="xs" textTransform="capitalize">
              ({runState})
            </Text>
          ) : undefined}
        </HStack>
        {reference !== undefined && interval !== undefined ? (
          <Text color="fg.muted" fontSize="xs">
            {translate("overview.deadlines.completionRule", { interval, reference })}
          </Text>
        ) : undefined}
        {contextLine !== undefined && (
          <Text color="fg.error" fontSize="xs">
            {contextLine}
          </Text>
        )}
      </VStack>
      <Time datetime={deadline.deadline_time} fontSize="sm" />
    </HStack>
  );
};
