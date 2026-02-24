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
import { Button, HStack, Link, Text } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiDatabase } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import { useAssetServiceNextRunAssets } from "openapi/queries";
import { AssetExpression, type ExpressionType } from "src/components/AssetExpression";
import type { NextRunEvent } from "src/components/AssetExpression/types";
import { TruncatedText } from "src/components/TruncatedText";
import { Popover } from "src/components/ui";

import { PartitionScheduleModal } from "./PartitionScheduleModal";

type Props = {
  readonly assetExpression?: ExpressionType | null;
  readonly dagId: string;
  readonly latestRunAfter?: string;
  readonly timetableSummary: string | null;
};

type PartitionScheduleProps = {
  readonly dagId: string;
  readonly isLoading: boolean;
  readonly pendingCount: number;
};

const PartitionSchedule = ({ dagId, isLoading, pendingCount }: PartitionScheduleProps) => {
  const { t: translate } = useTranslation("common");
  const [open, setOpen] = useState(false);

  return (
    <>
      <Button loading={isLoading} onClick={() => setOpen(true)} paddingInline={0} size="sm" variant="ghost">
        <FiDatabase style={{ display: "inline" }} />
        {translate("pendingDagRun", { count: pendingCount })}
      </Button>
      <PartitionScheduleModal dagId={dagId} onClose={() => setOpen(false)} open={open} />
    </>
  );
};

export const AssetSchedule = ({ assetExpression, dagId, latestRunAfter, timetableSummary }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const { data: nextRun, isLoading } = useAssetServiceNextRunAssets({ dagId });

  const isPartitioned = timetableSummary === "Partitioned Asset";

  const nextRunEvents = (nextRun?.events ?? []) as Array<NextRunEvent>;

  const pendingEvents = nextRunEvents.filter((ev) => {
    if (ev.lastUpdate === null) {
      return false;
    }
    if (isPartitioned) {
      return true;
    }

    return latestRunAfter !== undefined && dayjs(ev.lastUpdate).isAfter(latestRunAfter);
  });

  if (!nextRunEvents.length) {
    return (
      <HStack>
        <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
        <Text>{timetableSummary}</Text>
      </HStack>
    );
  }

  if (isPartitioned) {
    const pendingCount = (nextRun?.pending_partition_count as number | undefined) ?? 0;

    if (pendingCount === 0) {
      return (
        <HStack>
          <FiDatabase style={{ display: "inline", flexShrink: 0 }} />
          <Text>{translate("common:runTypes.asset_triggered")}</Text>
        </HStack>
      );
    }

    if (pendingCount > 1) {
      return <PartitionSchedule dagId={dagId} isLoading={isLoading} pendingCount={pendingCount} />;
    }

    // pendingCount === 1: fall through to the standard asset popover below
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
