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

import type { WebhookCancelRequestResponse, WebhookDeliveryResponse } from "openapi/requests/types.gen";

import { ClipboardIconButton, ClipboardRoot } from "src/system-components";

import RenderedJsonField from "src/components/RenderedJsonField";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

import { getTaskInstanceLink } from "src/utils/links";

import {
  type CancelScope,
  getCancelRequestStatusExplanation,
  getCancelRequestStatusLabel,
  getCancelScopeLabel,
  getOutcomeExplanation,
  getOutcomeLabel,
  getRunNotCancelledExplanation,
  getSkipReasonExplanation,
} from "./outcomes";

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

type RecordedWait = {
  dag_id: string;
  map_index: number;
  region_id?: string;
  region_index?: number;
  run_id: string;
  task_id: string;
  ti_id: string;
  wait_id: string;
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

const isRecordedWait = (value: unknown): value is RecordedWait =>
  typeof value === "object" &&
  value !== null &&
  ["dag_id", "run_id", "task_id", "ti_id", "wait_id"].every(
    (key) => typeof (value as Record<string, unknown>)[key] === "string",
  ) &&
  typeof (value as { map_index?: unknown }).map_index === "number" &&
  // A task instance in a dynamic region is recorded with both, any other with neither.
  ((typeof (value as { region_id?: unknown }).region_id === "string" &&
    typeof (value as { region_index?: unknown }).region_index === "number") ||
    ((value as { region_id?: unknown }).region_id === undefined &&
      (value as { region_index?: unknown }).region_index === undefined));

type RecordedCancelRequest = {
  dag_id: string;
  request_id: string;
  scope: CancelScope;
};

const isRecordedCancelRequest = (value: unknown): value is RecordedCancelRequest =>
  typeof value === "object" &&
  value !== null &&
  typeof (value as { dag_id?: unknown }).dag_id === "string" &&
  typeof (value as { request_id?: unknown }).request_id === "string" &&
  ((value as { scope?: unknown }).scope === "run" || (value as { scope?: unknown }).scope === "task");

const findList = <T,>(value: unknown, isItem: (item: unknown) => item is T): Array<T> | undefined =>
  Array.isArray(value) && value.every(isItem) ? value : undefined;

// Topic events, asset route events, skipped asset routes, webhook waits and cancel requests are each
// recorded as a list of objects. Every other key, and those lists in any other shape, are shown as recorded.
const splitActions = (actions: NonNullable<WebhookDeliveryResponse["actions"]>) => {
  const assetEvents = findList(actions.asset_events, isRecordedAssetEvent);
  const routeEvents = findList(actions.asset_routes, isRecordedRouteEvent);
  const skippedRoutes = findList(actions.skipped_asset_routes, isSkippedAssetRoute);
  const waits = findList(actions.waits, isRecordedWait);
  const cancelRequests = findList(actions.cancel_requests, isRecordedCancelRequest);
  const shownKeys = new Set([
    ...(assetEvents ? ["asset_events"] : []),
    ...(routeEvents ? ["asset_routes"] : []),
    ...(skippedRoutes ? ["skipped_asset_routes"] : []),
    ...(waits ? ["waits"] : []),
    ...(cancelRequests ? ["cancel_requests"] : []),
  ]);

  return {
    assetEvents: assetEvents ?? [],
    cancelRequests: cancelRequests ?? [],
    otherActions: Object.fromEntries(Object.entries(actions).filter(([key]) => !shownKeys.has(key))),
    routeEvents: routeEvents ?? [],
    skippedRoutes: skippedRoutes ?? [],
    waits: waits ?? [],
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

const CancelRequestResult = ({
  dagId,
  request,
}: {
  readonly dagId: string;
  readonly request: WebhookCancelRequestResponse;
}) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const dagRuns = request.result?.dag_runs ?? [];
  const taskInstances = request.result?.task_instances ?? [];
  const skippedRuns = request.result?.skipped_runs ?? [];

  return (
    <>
      {dagRuns.length > 0 ? (
        <VStack alignItems="start" gap={0}>
          <Text color="fg.muted">{translate("webhookDeliveries.details.cancelledRuns")}</Text>
          <List.Root data-testid="cancelled-runs" ps={4}>
            {dagRuns.map(({ run_id: runId, state }) => (
              <List.Item key={runId}>
                <HStack gap={1}>
                  <StateBadge state={state} />
                  <Link asChild color="fg.info">
                    <RouterLink to={`/dags/${dagId}/runs/${runId}`}>{runId}</RouterLink>
                  </Link>
                </HStack>
              </List.Item>
            ))}
          </List.Root>
        </VStack>
      ) : undefined}
      {taskInstances.length > 0 ? (
        <VStack alignItems="start" gap={0}>
          <Text color="fg.muted">{translate("webhookDeliveries.details.cancelledTaskInstances")}</Text>
          <List.Root data-testid="cancelled-task-instances" ps={4}>
            {taskInstances.map((taskInstance) => (
              <List.Item
                key={`${taskInstance.run_id}/${taskInstance.task_id}/${taskInstance.map_index}/${taskInstance.region_id ?? ""}/${taskInstance.region_index ?? ""}`}
              >
                <HStack gap={1}>
                  <StateBadge state={taskInstance.state} />
                  <Link asChild color="fg.info">
                    <RouterLink
                      to={getTaskInstanceLink({
                        dag_id: dagId,
                        dag_run_id: taskInstance.run_id,
                        map_index: taskInstance.map_index,
                        region_id: taskInstance.region_id,
                        region_index: taskInstance.region_index,
                        task_id: taskInstance.task_id,
                      })}
                    >
                      {taskInstance.map_index >= 0
                        ? `${taskInstance.task_id} [${taskInstance.map_index}]`
                        : taskInstance.task_id}
                    </RouterLink>
                  </Link>
                  <Text color="fg.muted" wordBreak="break-all">
                    {taskInstance.run_id}
                  </Text>
                </HStack>
              </List.Item>
            ))}
          </List.Root>
        </VStack>
      ) : undefined}
      {skippedRuns.length > 0 ? (
        <VStack alignItems="start" gap={0}>
          <Text color="fg.muted">{translate("webhookDeliveries.details.runsNotCancelled")}</Text>
          <List.Root data-testid="runs-not-cancelled" ps={4}>
            {skippedRuns.map(({ reason, run_id: runId }) => (
              <List.Item key={runId}>
                <VStack alignItems="start" gap={0}>
                  <Link asChild color="fg.info">
                    <RouterLink to={`/dags/${dagId}/runs/${runId}`}>{runId}</RouterLink>
                  </Link>
                  <Text color="fg.muted">{getRunNotCancelledExplanation(translate, reason)}</Text>
                </VStack>
              </List.Item>
            ))}
          </List.Root>
        </VStack>
      ) : undefined}
      {request.status === "applied" &&
      dagRuns.length === 0 &&
      taskInstances.length === 0 &&
      skippedRuns.length === 0 ? (
        <Text color="fg.muted">{translate("webhookDeliveries.details.noRunCancelled")}</Text>
      ) : undefined}
    </>
  );
};

const CancelRequestItem = ({
  dagId,
  request,
  scope,
}: {
  readonly dagId: string;
  readonly request: WebhookCancelRequestResponse | undefined;
  readonly scope: CancelScope;
}) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const tasks = request?.targets.tasks ?? [];

  return (
    <VStack alignItems="start" gap={0}>
      <Link asChild color="fg.info">
        <RouterLink to={`/dags/${dagId}`}>{dagId}</RouterLink>
      </Link>
      <LabeledText
        label={translate("webhookDeliveries.details.cancelScope")}
        value={getCancelScopeLabel(translate, scope)}
      />
      {tasks.length > 0 ? (
        <LabeledText
          label={translate("webhookDeliveries.details.cancelTasks")}
          value={tasks
            .map(
              ({ cancel_state: cancelState, task_id: taskId }) =>
                `${taskId} (${translate(`common:states.${cancelState}`)})`,
            )
            .join(", ")}
        />
      ) : undefined}
      {request === undefined ? undefined : (
        <LabeledText
          label={translate("webhookDeliveries.details.cancelRequestStatus")}
          value={getCancelRequestStatusLabel(translate, request.status)}
        />
      )}
      <Text color="fg.muted">{getCancelRequestStatusExplanation(translate, request?.status)}</Text>
      {request?.applied_at === null || request?.applied_at === undefined ? undefined : (
        <HStack gap={1}>
          <Text color="fg.muted">
            {translate(
              request.status === "failed"
                ? "webhookDeliveries.details.failedAt"
                : "webhookDeliveries.details.appliedAt",
            )}
          </Text>
          <Time datetime={request.applied_at} />
        </HStack>
      )}
      {request?.result?.error === null || request?.result?.error === undefined ? undefined : (
        <LabeledText
          label={translate("webhookDeliveries.details.cancelError")}
          value={request.result.error}
        />
      )}
      {request === undefined ? undefined : <CancelRequestResult dagId={dagId} request={request} />}
    </VStack>
  );
};

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
  const { assetEvents, cancelRequests, otherActions, routeEvents, skippedRoutes, waits } = splitActions(
    delivery.actions ?? {},
  );
  // The records are authoritative: actions can miss a request that a concurrent redelivery recorded,
  // and still names a request whose record was removed.
  const requests = delivery.cancel_requests ?? [];
  const requestIds = new Set(requests.map(({ id }) => id));
  const goneRequests = cancelRequests.filter(({ request_id: requestId }) => !requestIds.has(requestId));

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
            {waits.length > 0 ? (
              <VStack alignItems="start" gap={1}>
                <Text fontWeight="medium">{translate("webhookDeliveries.details.waits")}</Text>
                <List.Root data-testid="delivery-waits" ps={4}>
                  {waits.map((wait) => (
                    <List.Item key={wait.wait_id}>
                      <VStack alignItems="start" gap={0}>
                        <Link asChild color="fg.info">
                          <RouterLink
                            to={getTaskInstanceLink({
                              dag_id: wait.dag_id,
                              dag_run_id: wait.run_id,
                              map_index: wait.map_index,
                              region_id: wait.region_id,
                              region_index: wait.region_index,
                              task_id: wait.task_id,
                            })}
                          >
                            {wait.map_index >= 0 ? `${wait.task_id} [${wait.map_index}]` : wait.task_id}
                          </RouterLink>
                        </Link>
                        <LabeledText label={translate("common:dagId")} value={wait.dag_id} />
                        <LabeledText label={translate("common:runId")} value={wait.run_id} />
                        <LabeledText
                          label={translate("webhookDeliveries.details.waitId")}
                          value={wait.wait_id}
                        />
                      </VStack>
                    </List.Item>
                  ))}
                </List.Root>
              </VStack>
            ) : undefined}
            {requests.length > 0 || goneRequests.length > 0 ? (
              <VStack alignItems="start" gap={1}>
                <Text fontWeight="medium">{translate("webhookDeliveries.details.cancelRequests")}</Text>
                <List.Root data-testid="delivery-cancel-requests" gap={2} ps={4}>
                  {requests.map((request) => (
                    <List.Item key={request.id}>
                      <CancelRequestItem
                        dagId={request.dag_id}
                        request={request}
                        scope={request.targets.run === null ? "task" : "run"}
                      />
                    </List.Item>
                  ))}
                  {goneRequests.map((recorded) => (
                    <List.Item key={recorded.request_id}>
                      <CancelRequestItem dagId={recorded.dag_id} request={undefined} scope={recorded.scope} />
                    </List.Item>
                  ))}
                </List.Root>
              </VStack>
            ) : undefined}
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
