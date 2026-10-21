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
import { Box, Heading, Link, Table, Text, VStack } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams } from "react-router-dom";

import { useTaskInstanceServiceGetWebhookWaits } from "openapi/queries";
import type { TaskInstanceState, WebhookWaitResponse } from "openapi/requests/types.gen";

import Time from "src/components/Time";

import { useTaskInstanceCoordinates } from "src/hooks/useTaskInstanceCoordinates";

type WebhookWaitProps = {
  readonly refetchInterval: number | false;
  readonly tryNumber: number | undefined;
  readonly tryState: TaskInstanceState | null | undefined;
};

// Nothing closes a wait when its try stops, so an open wait can outlive the try it would resume.
const OPEN_STATUSES = new Set<WebhookWaitResponse["status"]>(["received", "registered"]);
const WAITING_STATES = new Set<TaskInstanceState | null>(["awaiting_input", "running"]);

const formatMatchValue = (translate: TFunction, value: WebhookWaitResponse["match"][string]) =>
  Array.isArray(value) && value.length > 1
    ? translate("taskInstance.webhookWait.oneOf", { values: value.join(", ") })
    : String(value);

/** The selected try's newest webhook wait, which is its open wait while it has one. */
export const WebhookWait = ({ refetchInterval, tryNumber, tryState }: WebhookWaitProps) => {
  const { t: translate } = useTranslation(["common", "browse"]);
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const coordinates = useTaskInstanceCoordinates();

  const { data } = useTaskInstanceServiceGetWebhookWaits(
    {
      ...coordinates,
      dagId,
      dagRunId: runId,
      limit: 1,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
      tryNumber,
    },
    undefined,
    { refetchInterval },
  );
  const wait = data?.webhook_waits[0];

  if (wait === undefined) {
    return undefined;
  }

  const tryStopped =
    tryState !== undefined && OPEN_STATUSES.has(wait.status) && !WAITING_STATES.has(tryState);

  return (
    <Box data-testid="webhook-wait" py={1}>
      <Heading py={1} size="sm">
        {translate("taskInstance.webhookWait.title")}
      </Heading>
      <Table.Root striped>
        <Table.Body>
          <Table.Row>
            <Table.Cell>{translate("browse:webhooks.webhook_one")}</Table.Cell>
            <Table.Cell>
              <Link asChild color="fg.info">
                <RouterLink to={`/webhook_deliveries/${wait.webhook}`}>{wait.webhook}</RouterLink>
              </Link>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.webhookWait.status")}</Table.Cell>
            <Table.Cell>
              <VStack alignItems="start" gap={1}>
                <Text>{translate(`taskInstance.webhookWait.statuses.${wait.status}`)}</Text>
                <Text color="fg.muted">
                  {tryStopped
                    ? translate("taskInstance.webhookWait.tryStopped")
                    : translate(`taskInstance.webhookWait.statusExplanations.${wait.status}`)}
                </Text>
              </VStack>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.webhookWait.events")}</Table.Cell>
            <Table.Cell>
              {wait.events === null
                ? translate("taskInstance.webhookWait.anyEvent")
                : wait.events.map(String).join(", ")}
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.webhookWait.match")}</Table.Cell>
            <Table.Cell>
              {Object.keys(wait.match).length === 0 ? (
                translate("taskInstance.webhookWait.anyMatch")
              ) : (
                <Table.Root data-testid="webhook-wait-match" size="sm">
                  <Table.Body>
                    {Object.entries(wait.match).map(([key, value]) => (
                      <Table.Row key={key}>
                        <Table.Cell>{key}</Table.Cell>
                        <Table.Cell>{formatMatchValue(translate, value)}</Table.Cell>
                      </Table.Row>
                    ))}
                  </Table.Body>
                </Table.Root>
              )}
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.webhookWait.deadline")}</Table.Cell>
            <Table.Cell>
              <Time datetime={wait.deadline} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.webhookWait.createdAt")}</Table.Cell>
            <Table.Cell>
              <Time datetime={wait.created_at} />
            </Table.Cell>
          </Table.Row>
          {wait.ended_at === null ? undefined : (
            <Table.Row>
              <Table.Cell>{translate("taskInstance.webhookWait.endedAt")}</Table.Cell>
              <Table.Cell>
                <Time datetime={wait.ended_at} />
              </Table.Cell>
            </Table.Row>
          )}
          {wait.inbound_delivery_id === null ? undefined : (
            <Table.Row>
              <Table.Cell>{translate("browse:webhookDeliveries.delivery_one")}</Table.Cell>
              <Table.Cell>
                <Link asChild color="fg.info">
                  <RouterLink to={`/webhook_deliveries/${wait.webhook}?id=${wait.inbound_delivery_id}`}>
                    {translate("taskInstance.webhookWait.viewDelivery")}
                  </RouterLink>
                </Link>
              </Table.Cell>
            </Table.Row>
          )}
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
