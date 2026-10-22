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
import type { ReactNode } from "react";

import { HStack, Link, List, Table, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import type { WebhookDeliveryResponse } from "openapi/requests/types.gen";

import { ClipboardIconButton, ClipboardRoot } from "src/system-components";

import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";

import { getOutcomeExplanation, getOutcomeLabel } from "./outcomes";

type RecordedAssetEvent = {
  asset: string;
  asset_id: number;
  event_id: number;
};

const isRecordedAssetEvent = (value: unknown): value is RecordedAssetEvent =>
  typeof value === "object" && value !== null && typeof (value as { asset?: unknown }).asset === "string";

// Asset events are recorded as a list of {asset, asset_id, event_id}. Every other key, and asset
// events in any other shape, are shown as recorded.
const splitActions = (actions: NonNullable<WebhookDeliveryResponse["actions"]>) => {
  const { asset_events: assetEvents, ...otherActions } = actions;

  return Array.isArray(assetEvents) && assetEvents.every(isRecordedAssetEvent)
    ? { assetEvents, otherActions }
    : { assetEvents: [], otherActions: actions };
};

const CopyableText = ({ value }: { readonly value: string }) => (
  <HStack gap={1}>
    <Text wordBreak="break-all">{value}</Text>
    <ClipboardRoot value={value}>
      <ClipboardIconButton />
    </ClipboardRoot>
  </HStack>
);

const DetailRow = ({ children, label }: { readonly children?: ReactNode; readonly label: string }) => (
  <Table.Row>
    <Table.Cell fontWeight="bold" verticalAlign="top" whiteSpace="nowrap">
      {label}
    </Table.Cell>
    <Table.Cell>{children}</Table.Cell>
  </Table.Row>
);

export const DeliveryDetails = ({ delivery }: { readonly delivery: WebhookDeliveryResponse }) => {
  const { t: translate } = useTranslation("browse");
  const { assetEvents, otherActions } = splitActions(delivery.actions ?? {});

  return (
    <Table.Root size="sm" striped>
      <Table.Body>
        <DetailRow label={translate("webhookDeliveries.details.id")}>
          <CopyableText value={delivery.id} />
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.columns.webhook")}>
          <Link asChild color="fg.info">
            <RouterLink to={`/webhook_deliveries/${delivery.webhook}`}>{delivery.webhook}</RouterLink>
          </Link>
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.columns.deliveryId")}>
          {delivery.delivery_id === null ? undefined : <CopyableText value={delivery.delivery_id} />}
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.columns.receivedAt")}>
          <Time datetime={delivery.received_at} />
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.columns.sourceType")}>
          {delivery.source_type}
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.columns.correlationKey")}>
          {delivery.correlation_key === null ? undefined : <CopyableText value={delivery.correlation_key} />}
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.columns.outcome")}>
          <VStack alignItems="start" gap={1}>
            <Text>{getOutcomeLabel(translate, delivery.outcome)}</Text>
            <Text color="fg.muted">{getOutcomeExplanation(translate, delivery.outcome)}</Text>
          </VStack>
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.details.attributes")}>
          {delivery.attributes === null ? undefined : (
            <Table.Root data-testid="delivery-attributes" size="sm">
              <Table.Body>
                {Object.entries(delivery.attributes).map(([key, value]) => (
                  <Table.Row key={key}>
                    <Table.Cell>{key}</Table.Cell>
                    <Table.Cell>{value}</Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table.Root>
          )}
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.details.candidates")}>
          <VStack alignItems="start" data-testid="delivery-candidates" gap={0}>
            {(delivery.candidates ?? []).map((name) => (
              <CopyableText key={name} value={name} />
            ))}
          </VStack>
        </DetailRow>
        <DetailRow label={translate("webhookDeliveries.details.actions")}>
          <VStack alignItems="start" gap={2}>
            {assetEvents.length > 0 ? (
              <VStack alignItems="start" gap={1}>
                <Text fontWeight="medium">{translate("webhookDeliveries.details.assetEvents")}</Text>
                <List.Root data-testid="delivery-asset-events" ps={4}>
                  {assetEvents.map(({ asset, event_id: eventId }) => (
                    <List.Item key={eventId}>{asset}</List.Item>
                  ))}
                </List.Root>
              </VStack>
            ) : undefined}
            {Object.keys(otherActions).length > 0 ? <RenderedJsonField content={otherActions} /> : undefined}
          </VStack>
        </DetailRow>
      </Table.Body>
    </Table.Root>
  );
};
