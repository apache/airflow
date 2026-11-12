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
import { useState, type PropsWithChildren } from "react";

import { Table } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import {
  useAssetServiceGetAssetEvents,
  useAssetServiceGetAssets,
  useTriggerServiceGetTrigger,
} from "openapi/queries";
import type { AssetResponse, CallbackState, TaskInstanceState } from "openapi/requests/types.gen";

import { RouterLink } from "src/system-components";

import { TriggerUsedBy } from "src/pages/Triggers/TriggerUsedBy";

import { AssetEvents } from "src/components/Assets/AssetEvents";
import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

import { useConfig } from "src/queries/useConfig";

const RECENT_EVENTS = 5;

type AssetRow = { row: { original: AssetResponse } };

const Row = ({ children, label }: PropsWithChildren<{ readonly label: string }>) => (
  <Table.Row>
    <Table.Cell whiteSpace="nowrap">{label}</Table.Cell>
    <Table.Cell>{children}</Table.Cell>
  </Table.Row>
);

const createColumns = (
  translate: TFunction,
  triggerId: number,
): Array<ColumnDef<DataTableFeatures, AssetResponse>> => [
  {
    accessorKey: "name",
    cell: ({ row: { original } }: AssetRow) => (
      <RouterLink to={`/assets/${original.id}`}>{original.name}</RouterLink>
    ),
    enableSorting: false,
    header: translate("asset_one"),
  },
  {
    accessorKey: "watchers",
    cell: ({ row: { original } }: AssetRow) =>
      original.watchers
        .filter((watcher) => watcher.trigger_id === triggerId)
        .map((watcher) => watcher.name)
        .join(", "),
    enableSorting: false,
    header: translate("watcher_one"),
  },
  {
    accessorKey: "last_asset_event",
    cell: ({ row: { original } }: AssetRow) => <Time datetime={original.last_asset_event?.timestamp} />,
    enableSorting: false,
    header: translate("lastAssetEvent"),
  },
];

/**
 * Each watched asset with its own latest event, so a busy asset cannot hide a quiet one's, then the
 * latest events of them all. Both requests name the trigger, not its assets, so neither grows with them.
 */
const WatchedAssets = ({
  hasWatchers,
  triggerId,
}: {
  readonly hasWatchers: boolean;
  readonly triggerId: number;
}) => {
  const { t: translate } = useTranslation("assets");
  const pageSize = useConfig("fallback_page_limit") as number;
  const [pageIndex, setPageIndex] = useState(0);
  const { data: assets, isLoading: isLoadingAssets } = useAssetServiceGetAssets(
    { limit: pageSize, offset: pageIndex * pageSize, triggerId },
    undefined,
    { enabled: hasWatchers, placeholderData: (prev) => prev },
  );
  const { data: events, isLoading: isLoadingEvents } = useAssetServiceGetAssetEvents(
    { limit: RECENT_EVENTS, orderBy: ["-timestamp"], triggerId },
    undefined,
    { enabled: hasWatchers },
  );

  return (
    <>
      <DataTable
        columns={createColumns(translate, triggerId)}
        data={assets?.assets ?? []}
        hideRowCountHeading
        initialState={{ pagination: { pageIndex, pageSize }, sorting: [] }}
        isLoading={isLoadingAssets}
        modelName="common:asset"
        onStateChange={(state) => setPageIndex(state.pagination.pageIndex)}
        total={assets?.total_entries ?? 0}
      />
      <AssetEvents data={events} isLoading={isLoadingEvents} px={0} />
    </>
  );
};

export const UsedBy = () => {
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
            <TriggerUsedBy trigger={trigger} />
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
            <TriggerUsedBy trigger={trigger} />
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

  return <WatchedAssets hasWatchers={watchers.length > 0} triggerId={trigger.id} />;
};
