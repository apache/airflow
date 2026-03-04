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
import { HStack, Text, Link } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useTranslation } from "react-i18next";
import { FiDatabase } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { useAssetServiceGetDagAssetQueuedEvents, useAssetServiceNextRunAssets } from "openapi/queries";
import { AssetExpression, type ExpressionType } from "src/components/AssetExpression";
import type { NextRunEvent } from "src/components/AssetExpression/types";
import { TruncatedText } from "src/components/TruncatedText";
import { Button, Popover } from "src/components/ui";

type Props = {
  readonly assetExpression?: ExpressionType | null;
  readonly dagId: string;
  readonly latestRunAfter?: string;
  readonly timetableSummary: string | null;
};

export const AssetSchedule = ({ assetExpression, dagId, latestRunAfter, timetableSummary }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);

  const { data: nextRun, isLoading: isNextRunLoading } = useAssetServiceNextRunAssets({ dagId });
  const {
    data: queuedEventsData,
    error: queuedEventsError,
    isLoading: isQueuedEventsLoading,
  } = useAssetServiceGetDagAssetQueuedEvents({ dagId }, undefined);

  const nextRunEvents = (nextRun?.events ?? []) as Array<NextRunEvent>;
  const queuedEventsErrorStatus =
    typeof queuedEventsError === "object" && queuedEventsError !== null && "status" in queuedEventsError
      ? (queuedEventsError as { status?: number }).status
      : undefined;
  const hasQueuedEventsError = Boolean(queuedEventsError) && queuedEventsErrorStatus !== 404;
  const queuedAssetEvents = new Map<number, string>();

  if (!hasQueuedEventsError) {
    for (const event of queuedEventsData?.queued_events ?? []) {
      // Keep a single event timestamp per asset, using the latest one when duplicates exist.
      const existingEventDate = queuedAssetEvents.get(event.asset_id);

      if (existingEventDate === undefined || dayjs(event.created_at).isAfter(existingEventDate)) {
        queuedAssetEvents.set(event.asset_id, event.created_at);
      }
    }
  }

  const pendingEvents = nextRunEvents.flatMap((event) => {
    if (hasQueuedEventsError) {
      if (event.lastUpdate === null) {
        return [];
      }

      return latestRunAfter !== undefined && dayjs(event.lastUpdate).isAfter(latestRunAfter) ? [event] : [];
    }

    const queuedAt = queuedAssetEvents.get(event.id);

    return queuedAt === undefined ? [] : [{ ...event, lastUpdate: event.lastUpdate ?? queuedAt }];
  });
  const isLoading = isNextRunLoading || isQueuedEventsLoading;

  if (!nextRunEvents.length) {
    return (
      <HStack>
        <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
        <Text>{timetableSummary}</Text>
      </HStack>
    );
  }

  const [asset] = nextRunEvents;

  if (nextRunEvents.length === 1 && asset !== undefined) {
    return (
      <HStack>
        <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
        <Link asChild color="fg.info" display="block" fontSize="sm">
          <RouterLink to={`/assets/${asset.id}`}>
            <TruncatedText minWidth={0} text={asset.name ?? asset.uri} />
          </RouterLink>
        </Link>
      </HStack>
    );
  }

  return (
    // eslint-disable-next-line jsx-a11y/no-autofocus
    <Popover.Root autoFocus={false} lazyMount positioning={{ placement: "bottom-end" }} unmountOnExit>
      <Popover.Trigger asChild>
        <Button loading={isLoading} paddingInline={0} size="sm" variant="ghost">
          <FiDatabase style={{ display: "inline" }} />
          {translate("assetSchedule", { count: pendingEvents.length, total: nextRunEvents.length })}
        </Button>
      </Popover.Trigger>
      <Popover.Content css={{ "--popover-bg": "colors.bg.emphasized" }} width="fit-content">
        <Popover.Arrow />
        <Popover.Body>
          <AssetExpression
            events={pendingEvents}
            expression={(nextRun?.asset_expression ?? assetExpression) as ExpressionType}
          />
        </Popover.Body>
      </Popover.Content>
    </Popover.Root>
  );
};
