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

import { Box, Heading, Table, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useAssetServiceGetAssetEvents, useTriggerServiceGetTrigger } from "openapi/queries";
import type {
  CallbackState,
  TaskInstanceState,
  TriggerAssetWatcherReferenceResponse,
} from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { TriggerOwner } from "src/pages/Triggers/TriggerOwner";

import { AssetEvents } from "src/components/Assets/AssetEvents";
import { StateBadge } from "src/components/StateBadge";

const RECENT_EVENTS = 5;

const Row = ({ children, label }: PropsWithChildren<{ readonly label: string }>) => (
  <Table.Row>
    <Table.Cell whiteSpace="nowrap">{label}</Table.Cell>
    <Table.Cell>{children}</Table.Cell>
  </Table.Row>
);

const WatchedAsset = ({
  assetId,
  watchers,
}: {
  readonly assetId: number;
  readonly watchers: Array<TriggerAssetWatcherReferenceResponse>;
}) => {
  const { data, isLoading } = useAssetServiceGetAssetEvents({
    assetId,
    limit: RECENT_EVENTS,
    orderBy: ["-timestamp"],
  });

  return (
    <Box>
      <Heading size="md">
        <RouterLink to={`/assets/${assetId}`}>{watchers[0]?.asset_name}</RouterLink>
      </Heading>
      <Text color="fg.muted" fontSize="sm">
        {watchers.map((watcher) => watcher.watcher_name).join(", ")}
      </Text>
      <AssetEvents assetId={assetId} data={data} isLoading={isLoading} px={0} />
    </Box>
  );
};

export const Owner = () => {
  const { t: translate } = useTranslation(["common", "dag"]);
  const { triggerId = "" } = useParams();
  const { data: trigger } = useTriggerServiceGetTrigger({ triggerId: Number(triggerId) });

  if (trigger === undefined) {
    return undefined;
  }

  const { asset_watchers: watchers, callback, task_instance: ti } = trigger;

  if (ti !== null) {
    return (
      <Table.Root striped>
        <Table.Body>
          <Row label={translate("taskInstance_one")}>
            <TriggerOwner trigger={trigger} />
          </Row>
          <Row label={translate("dagId")}>{ti.dag_id}</Row>
          <Row label={translate("runId")}>{ti.dag_run_id}</Row>
          <Row label={translate("taskId")}>{ti.task_id}</Row>
          {ti.map_index >= 0 ? <Row label={translate("mapIndex")}>{ti.map_index}</Row> : undefined}
          <Row label={translate("tryNumber")}>{ti.try_number}</Row>
          <Row label={translate("state")}>
            <StateBadge state={ti.state as TaskInstanceState | null}>
              {translate(`states.${ti.state ?? "no_status"}`)}
            </StateBadge>
          </Row>
        </Table.Body>
      </Table.Root>
    );
  }

  if (callback !== null) {
    const state = callback.state as CallbackState | null;

    return (
      <Table.Root striped>
        <Table.Body>
          <Row label={translate("dag:callbacks.callback_one")}>
            <TriggerOwner trigger={trigger} />
          </Row>
          {callback.dag_id === null ? undefined : <Row label={translate("dagId")}>{callback.dag_id}</Row>}
          {callback.dag_run_id === null ? undefined : (
            <Row label={translate("runId")}>{callback.dag_run_id}</Row>
          )}
          <Row label={translate("state")}>
            {/* "pending" is callback-only, so it borrows the "scheduled" badge style, as on the Callbacks tab. */}
            <StateBadge state={state === "pending" ? "scheduled" : state}>
              {translate(`states.${state ?? "no_status"}`)}
            </StateBadge>
          </Row>
        </Table.Body>
      </Table.Root>
    );
  }

  const assetIds = [...new Set(watchers.map((watcher) => watcher.asset_id))];

  return (
    <VStack align="stretch" gap={4}>
      {assetIds.map((assetId) => (
        <WatchedAsset
          assetId={assetId}
          key={assetId}
          watchers={watchers.filter((watcher) => watcher.asset_id === assetId)}
        />
      ))}
    </VStack>
  );
};
