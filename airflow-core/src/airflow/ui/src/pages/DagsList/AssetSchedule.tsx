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
import { Button, HStack, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiAlertTriangle, FiDatabase } from "react-icons/fi";

import { useAssetServiceGetDagAssetQueuedEvents, useAssetServiceNextRunAssets } from "openapi/queries";
import type { NextRunAssetEventResponse } from "openapi/requests/types.gen";
import { AssetExpression, type ExpressionType } from "src/components/AssetExpression";
import { RollupKeyChecklistPopover } from "src/components/RollupKeyChecklist";
import { TruncatedText } from "src/components/TruncatedText";
import { Popover, RouterLink } from "src/components/ui";
import { Tooltip } from "src/components/ui/Tooltip";

import { PartitionScheduleModal } from "./PartitionScheduleModal";

type Props = {
  readonly assetExpression?: ExpressionType | null;
  readonly dagId: string;
  readonly timetablePartitioned: boolean | null;
  readonly timetableSummary: string | null;
};

type PartitionScheduleProps = {
  readonly dagId: string;
  readonly hasInactiveAsset: boolean;
  readonly isLoading: boolean;
  readonly pendingCount: number;
};

const PartitionSchedule = ({ dagId, hasInactiveAsset, isLoading, pendingCount }: PartitionScheduleProps) => {
  const { t: translate } = useTranslation("common");
  const [open, setOpen] = useState(false);

  return (
    <>
      <Tooltip content={translate("common:assetInactive.tooltip")} disabled={!hasInactiveAsset}>
        <Button loading={isLoading} onClick={() => setOpen(true)} paddingInline={0} variant="ghost">
          {hasInactiveAsset ? (
            <FiAlertTriangle color="var(--chakra-colors-warning-fg)" style={{ display: "inline" }} />
          ) : (
            <FiDatabase style={{ display: "inline" }} />
          )}
          {translate("pendingDagRun", { count: pendingCount })}
        </Button>
      </Tooltip>
      <PartitionScheduleModal dagId={dagId} onClose={() => setOpen(false)} open={open} />
    </>
  );
};

export const AssetSchedule = ({ assetExpression, dagId, timetablePartitioned, timetableSummary }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);

  const { data: nextRun, isLoading: isNextRunLoading } = useAssetServiceNextRunAssets({ dagId });
  const { data: queuedEventsData, isLoading: isQueuedEventsLoading } = useAssetServiceGetDagAssetQueuedEvents(
    { dagId },
    undefined,
    { enabled: !timetablePartitioned },
  );

  const nextRunEvents: Array<NextRunAssetEventResponse> = nextRun?.events ?? [];
  const queuedAssetEvents = new Map<number, string>();

  if (!timetablePartitioned) {
    for (const event of queuedEventsData?.queued_events ?? []) {
      // Keep a single event timestamp per asset, using the latest one when duplicates exist.
      const existingEventDate = queuedAssetEvents.get(event.asset_id);

      if (existingEventDate === undefined || dayjs(event.created_at).isAfter(existingEventDate)) {
        queuedAssetEvents.set(event.asset_id, event.created_at);
      }
    }
  }

  // Fully satisfied assets (used for the button count label).
  const pendingEvents = nextRunEvents.flatMap((event) => {
    if (timetablePartitioned) {
      return event.last_update === null ? [] : [event];
    }

    const queuedAt = queuedAssetEvents.get(event.id);

    return queuedAt === undefined ? [] : [{ ...event, last_update: event.last_update ?? queuedAt }];
  });

  // For partitioned Dags, also include partially-received assets in the popover visualization.
  const popoverEvents = timetablePartitioned
    ? nextRunEvents.filter((event) => (event.received_count ?? (event.last_update === null ? 0 : 1)) > 0)
    : pendingEvents;

  // For partitioned Dags (which may use rollup mappers), compute event-level totals so the
  // button label reflects received/required partition-key events, not just asset counts.
  // For non-partitioned Dags, fall back to asset counts (existing behaviour).
  const scheduledCount = timetablePartitioned
    ? nextRunEvents.reduce(
        (sum, event) => sum + Math.min(event.received_count ?? 0, event.required_count ?? 1),
        0,
      )
    : pendingEvents.length;
  const scheduledTotal = timetablePartitioned
    ? nextRunEvents.reduce((sum, event) => sum + (event.required_count ?? 1), 0)
    : nextRunEvents.length;

  const isLoading = isNextRunLoading || (!timetablePartitioned && isQueuedEventsLoading);

  if (!nextRunEvents.length) {
    return (
      <HStack>
        <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
        <Text>{timetableSummary}</Text>
      </HStack>
    );
  }

  if (timetablePartitioned) {
    const pendingCount = nextRun?.pending_partition_count ?? 0;
    const hasInactiveAsset = nextRunEvents.some((event) => event.asset_inactive === true);

    if (pendingCount === 0) {
      return (
        <HStack>
          <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
          <Text>{translate("common:runTypes.asset_triggered")}</Text>
        </HStack>
      );
    }

    if (pendingCount > 1) {
      return (
        <PartitionSchedule
          dagId={dagId}
          hasInactiveAsset={hasInactiveAsset}
          isLoading={isLoading}
          pendingCount={pendingCount}
        />
      );
    }

    // pendingCount === 1: render single-asset view with inactive warning.
    const [partitionedAsset] = nextRunEvents;

    if (nextRunEvents.length === 1 && partitionedAsset !== undefined) {
      const requiredCount = partitionedAsset.required_count ?? 1;
      const receivedCount = partitionedAsset.received_count ?? 0;
      const requiredKeys = partitionedAsset.required_keys ?? [];

      if (partitionedAsset.is_rollup && requiredKeys.length > 0) {
        return (
          <Tooltip content={translate("common:assetInactive.tooltip")} disabled={!hasInactiveAsset}>
            <HStack>
              {hasInactiveAsset ? (
                <FiAlertTriangle
                  color="var(--chakra-colors-warning-fg)"
                  style={{ display: "inline", flexShrink: 0 }}
                />
              ) : (
                <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
              )}
              <RouterLink display="block" fontSize="sm" to={`/assets/${partitionedAsset.id}`}>
                <TruncatedText minWidth={0} text={partitionedAsset.name ?? partitionedAsset.uri} />
              </RouterLink>
              <RollupKeyChecklistPopover
                isLoading={isLoading}
                receivedCount={receivedCount}
                receivedKeys={partitionedAsset.received_keys ?? []}
                requiredCount={requiredCount}
                requiredKeys={requiredKeys}
              />
            </HStack>
          </Tooltip>
        );
      }

      return (
        <Tooltip content={translate("common:assetInactive.tooltip")} disabled={!hasInactiveAsset}>
          <HStack>
            {hasInactiveAsset ? (
              <FiAlertTriangle
                color="var(--chakra-colors-warning-fg)"
                style={{ display: "inline", flexShrink: 0 }}
              />
            ) : (
              <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
            )}
            <RouterLink display="block" fontSize="sm" to={`/assets/${partitionedAsset.id}`}>
              <TruncatedText minWidth={0} text={partitionedAsset.name ?? partitionedAsset.uri} />
            </RouterLink>
          </HStack>
        </Tooltip>
      );
    }
  }

  const [asset] = nextRunEvents;

  if (nextRunEvents.length === 1 && asset !== undefined) {
    const requiredCount = asset.required_count ?? 1;
    const receivedCount = asset.received_count ?? 0;
    const requiredKeys = asset.required_keys ?? [];

    if (asset.is_rollup && requiredKeys.length > 0) {
      return (
        <HStack>
          <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
          <RouterLink display="block" fontSize="sm" to={`/assets/${asset.id}`}>
            <TruncatedText minWidth={0} text={asset.name ?? asset.uri} />
          </RouterLink>
          <RollupKeyChecklistPopover
            isLoading={isLoading}
            receivedCount={receivedCount}
            receivedKeys={asset.received_keys ?? []}
            requiredCount={requiredCount}
            requiredKeys={requiredKeys}
          />
        </HStack>
      );
    }

    return (
      <HStack>
        <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
        <RouterLink display="block" fontSize="sm" to={`/assets/${asset.id}`}>
          <TruncatedText minWidth={0} text={asset.name ?? asset.uri} />
        </RouterLink>
      </HStack>
    );
  }

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Button loading={isLoading} paddingInline={0} variant="ghost">
          <FiDatabase style={{ display: "inline" }} />
          {translate("assetSchedule", { count: scheduledCount, total: scheduledTotal })}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <AssetExpression
            events={popoverEvents}
            expression={nextRun?.asset_expression ?? assetExpression ?? undefined}
          />
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
