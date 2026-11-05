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
import type { PropsWithChildren } from "react";

import { HStack, Table, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTriggerServiceGetTrigger } from "openapi/queries";

import { ClipboardIconButton, ClipboardRoot } from "src/system-components";

import { TriggerOwner } from "src/pages/Triggers/TriggerOwner";

import { TeamName } from "src/components/TeamName";
import Time from "src/components/Time";
import { TriggerStatusBadge } from "src/components/TriggerStatusBadge";

import { useShowTeam } from "src/hooks/useShowTeam";

const Row = ({ children, label }: PropsWithChildren<{ readonly label: string }>) => (
  <Table.Row>
    <Table.Cell whiteSpace="nowrap">{label}</Table.Cell>
    <Table.Cell>{children}</Table.Cell>
  </Table.Row>
);

export const Details = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const { triggerId = "" } = useParams();
  const { data: trigger } = useTriggerServiceGetTrigger({ triggerId: Number(triggerId) });
  const showTeam = useShowTeam(trigger?.team_name);

  if (trigger === undefined) {
    return undefined;
  }

  // The count is cleared when the trigger recovers; the last error is kept, muted.
  const unrecovered = trigger.consecutive_failures > 0;

  return (
    <Table.Root striped>
      <Table.Body>
        <Row label={translate("common:taskInstance.triggerer.id")}>{trigger.id}</Row>
        <Row label={translate("triggers.columns.kind")}>{translate(`triggers.kind.${trigger.kind}`)}</Row>
        <Row label={translate("triggers.columns.status")}>
          <TriggerStatusBadge status={trigger.status} />
        </Row>
        <Row label={translate("common:taskInstance.triggerer.class")}>
          <HStack>
            <Text fontFamily="mono" wordBreak="break-all">
              {trigger.classpath}
            </Text>
            <ClipboardRoot value={trigger.classpath}>
              <ClipboardIconButton />
            </ClipboardRoot>
          </HStack>
        </Row>
        {trigger.queue === null ? undefined : (
          <Row label={translate("common:taskInstance.queue")}>{trigger.queue}</Row>
        )}
        {showTeam ? (
          <Row label={translate("common:dagDetails.team")}>
            <TeamName teamName={trigger.team_name} />
          </Row>
        ) : undefined}
        <Row label={translate("triggers.columns.owner")}>
          <TriggerOwner trigger={trigger} />
        </Row>
        <Row label={translate("triggers.columns.triggerer")}>{trigger.triggerer_hostname}</Row>
        <Row label={translate("triggers.columns.failures")}>{trigger.consecutive_failures}</Row>
        {unrecovered ? (
          <>
            <Row label={translate("triggers.detail.firstFailedAt")}>
              <Time datetime={trigger.first_failed_at} />
            </Row>
            <Row label={translate("triggers.detail.lastFailedAt")}>
              <Time datetime={trigger.last_failed_at} />
            </Row>
          </>
        ) : undefined}
        {trigger.last_error === null ? undefined : (
          <Row label={translate("triggers.columns.lastError")}>
            <Text
              color={unrecovered ? "fg.error" : "fg.muted"}
              data-testid="trigger-last-error"
              fontFamily="mono"
              whiteSpace="pre-wrap"
              wordBreak="break-word"
            >
              {trigger.last_error}
            </Text>
          </Row>
        )}
        <Row label={translate("triggers.columns.lastEvent")}>
          <Time datetime={trigger.last_event_at} />
        </Row>
        <Row label={translate("common:table.createdAt")}>
          <Time datetime={trigger.created_date} />
        </Row>
      </Table.Body>
    </Table.Root>
  );
};
