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

import { getOutcomeExplanation, getOutcomeLabel, getSkipReasonExplanation } from "./outcomes";

type RecordedAssetEvent = {
  asset: string;
  asset_id: number;
  event_id: number;
};

type RecordedRouteEvent = {
  partition_key: string | null;
  topic: string;
} & RecordedAssetEvent;

type SkippedAssetRoute = {
  asset: string;
  partition_key_field: string | null;
  reason: string;
  topic: string;
};

const isRecordedAssetEvent = (value: unknown): value is RecordedAssetEvent =>
  typeof value === "object" &&
  value !== null &&
  typeof (value as { asset?: unknown }).asset === "string" &&
  typeof (value as { asset_id?: unknown }).asset_id === "number";

const isRecordedRouteEvent = (value: unknown): value is RecordedRouteEvent =>
  isRecordedAssetEvent(value) &&
  typeof (value as { topic?: unknown }).topic === "string" &&
  ((value as { partition_key?: unknown }).partition_key === null ||
    typeof (value as { partition_key?: unknown }).partition_key === "string");

const isSkippedAssetRoute = (value: unknown): value is SkippedAssetRoute =>
  typeof value === "object" &&
  value !== null &&
  typeof (value as { asset?: unknown }).asset === "string" &&
  typeof (value as { reason?: unknown }).reason === "string" &&
  typeof (value as { topic?: unknown }).topic === "string" &&
  ((value as { partition_key_field?: unknown }).partition_key_field === null ||
    typeof (value as { partition_key_field?: unknown }).partition_key_field === "string");

const findList = <T,>(value: unknown, isItem: (item: unknown) => item is T): Array<T> | undefined =>
  Array.isArray(value) && value.every(isItem) ? value : undefined;

// Topic events, asset route events and skipped asset routes are each recorded as a list of objects.
// Every other key, and those lists in any other shape, are shown as recorded.
const splitActions = (actions: NonNullable<WebhookDeliveryResponse["actions"]>) => {
  const assetEvents = findList(actions.asset_events, isRecordedAssetEvent);
  const routeEvents = findList(actions.asset_routes, isRecordedRouteEvent);
  const skippedRoutes = findList(actions.skipped_asset_routes, isSkippedAssetRoute);
  const shownKeys = new Set([
    ...(assetEvents ? ["asset_events"] : []),
    ...(routeEvents ? ["asset_routes"] : []),
    ...(skippedRoutes ? ["skipped_asset_routes"] : []),
  ]);

  return {
    assetEvents: assetEvents ?? [],
    otherActions: Object.fromEntries(Object.entries(actions).filter(([key]) => !shownKeys.has(key))),
    routeEvents: routeEvents ?? [],
    skippedRoutes: skippedRoutes ?? [],
  };
};

const CopyableText = ({ value }: { readonly value: string }) => (
  <HStack gap={1}>
    <Text wordBreak="break-all">{value}</Text>
    <ClipboardRoot value={value}>
      <ClipboardIconButton />
    </ClipboardRoot>
  </HStack>
);

const LabeledText = ({ label, value }: { readonly label: string; readonly value: string }) => (
  <HStack gap={1}>
    <Text color="fg.muted">{label}</Text>
    <Text wordBreak="break-all">{value}</Text>
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
  const { t: translate } = useTranslation(["browse", "common"]);
  const { assetEvents, otherActions, routeEvents, skippedRoutes } = splitActions(delivery.actions ?? {});

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
                  {assetEvents.map(({ asset, asset_id: assetId, event_id: eventId }) => (
                    <List.Item key={eventId}>
                      <Link asChild color="fg.info">
                        <RouterLink to={`/assets/${assetId}`}>{asset}</RouterLink>
                      </Link>
                    </List.Item>
                  ))}
                </List.Root>
              </VStack>
            ) : undefined}
            {routeEvents.length > 0 ? (
              <VStack alignItems="start" gap={1}>
                <Text fontWeight="medium">{translate("webhookDeliveries.details.assetRouteEvents")}</Text>
                <List.Root data-testid="delivery-asset-route-events" ps={4}>
                  {routeEvents.map(
                    ({ asset, asset_id: assetId, event_id: eventId, partition_key: partitionKey, topic }) => (
                      <List.Item key={eventId}>
                        <VStack alignItems="start" gap={0}>
                          <Link asChild color="fg.info">
                            <RouterLink to={`/assets/${assetId}`}>{asset}</RouterLink>
                          </Link>
                          <LabeledText label={translate("webhookDeliveries.details.topic")} value={topic} />
                          {partitionKey === null ? undefined : (
                            <LabeledText
                              label={translate("common:dagRun.partitionKey")}
                              value={partitionKey}
                            />
                          )}
                        </VStack>
                      </List.Item>
                    ),
                  )}
                </List.Root>
              </VStack>
            ) : undefined}
            {skippedRoutes.length > 0 ? (
              <VStack alignItems="start" gap={1}>
                <Text fontWeight="medium">{translate("webhookDeliveries.details.skippedAssetRoutes")}</Text>
                <List.Root data-testid="delivery-skipped-asset-routes" ps={4}>
                  {skippedRoutes.map(
                    ({ asset, partition_key_field: partitionKeyField, reason, topic }, index) => (
                      // eslint-disable-next-line react/no-array-index-key -- routes can be identical, and a record never reorders
                      <List.Item key={index}>
                        <VStack alignItems="start" gap={0}>
                          <Text wordBreak="break-all">{asset}</Text>
                          <LabeledText label={translate("webhookDeliveries.details.topic")} value={topic} />
                          {partitionKeyField === null ? undefined : (
                            <LabeledText
                              label={translate("webhookDeliveries.details.partitionKeyField")}
                              value={partitionKeyField}
                            />
                          )}
                          <Text color="fg.muted">{getSkipReasonExplanation(translate, reason)}</Text>
                        </VStack>
                      </List.Item>
                    ),
                  )}
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
