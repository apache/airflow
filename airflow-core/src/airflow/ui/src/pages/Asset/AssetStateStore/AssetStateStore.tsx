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
import { Flex, Link, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams } from "react-router-dom";

import { useAssetStateStoreServiceListAssetStateStore } from "openapi/queries";
import type { AssetStateStoreLastUpdatedBy, AssetStateStoreResponse } from "openapi/requests";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StoreValueCell } from "src/components/StoreValueCell";
import Time from "src/components/Time";
import { getTaskInstanceLink } from "src/utils/links";

import { AddAssetStateStoreButton } from "./AddAssetStateStoreButton";
import { ClearAllAssetStateStoreButton } from "./ClearAllAssetStateStoreButton";
import { DeleteAssetStateStoreButton } from "./DeleteAssetStateStoreButton";
import { EditAssetStateStoreButton } from "./EditAssetStateStoreButton";

type TaskWriter = { dag_id: string; run_id: string; task_id: string } & AssetStateStoreLastUpdatedBy;

const isTaskWriter = (writer: AssetStateStoreLastUpdatedBy): writer is TaskWriter =>
  writer.kind === "task" &&
  writer.dag_id !== null &&
  writer.dag_id !== undefined &&
  writer.run_id !== null &&
  writer.run_id !== undefined &&
  writer.task_id !== null &&
  writer.task_id !== undefined;

type ColumnsProps = {
  readonly assetId: number;
  readonly translate: (key: string) => string;
};

const getColumns = ({ assetId, translate }: ColumnsProps): Array<ColumnDef<AssetStateStoreResponse>> => [
  {
    accessorKey: "key",
    cell: ({ row: { original } }) => <Text>{original.key}</Text>,
    header: translate("common:key"),
  },
  {
    accessorKey: "value",
    cell: ({ row: { original } }) => <StoreValueCell value={original.value} />,
    enableSorting: false,
    header: translate("common:value"),
  },
  {
    accessorKey: "updated_at",
    cell: ({ row: { original } }) => <Time datetime={original.updated_at} />,
    header: translate("common:table.updatedAt"),
  },
  {
    accessorKey: "last_updated_by",
    cell: ({ row: { original } }) => {
      const writer = original.last_updated_by;

      if (!writer) {
        return <Text color="fg.muted">—</Text>;
      }
      if (isTaskWriter(writer)) {
        const path = getTaskInstanceLink({
          dagId: writer.dag_id,
          dagRunId: writer.run_id,
          mapIndex: writer.map_index ?? undefined,
          taskId: writer.task_id,
        });

        return (
          <Flex direction="column">
            <Link asChild color="fg.info">
              <RouterLink to={path}>{writer.task_id}</RouterLink>
            </Link>
            <Text color="fg.muted" fontSize="xs">
              {writer.dag_id}
            </Text>
          </Flex>
        );
      }

      return (
        <Text>
          {writer.kind === "api"
            ? translate("assets:assetStateStore.lastUpdatedByApi")
            : translate("assets:assetStateStore.lastUpdatedByWatcher")}
        </Text>
      );
    },
    enableSorting: false,
    header: translate("assets:assetStateStore.lastUpdatedBy"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditAssetStateStoreButton assetId={assetId} storeKey={original.key} />
        <DeleteAssetStateStoreButton assetId={assetId} storeKey={original.key} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
  },
];

export const AssetStateStore = () => {
  const { t: translate } = useTranslation(["assets", "common"]);
  const { assetId: rawAssetId } = useParams();
  const assetId = rawAssetId === undefined ? 0 : parseInt(rawAssetId, 10);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;

  const { data, error, isFetching, isLoading } = useAssetStateStoreServiceListAssetStateStore({
    assetId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
  });

  const columns = getColumns({ assetId, translate });

  return (
    <>
      <Flex gap={2} justifyContent="flex-end" mb={2}>
        <AddAssetStateStoreButton assetId={assetId} />
        {(data?.total_entries ?? 0) > 0 ? <ClearAllAssetStateStoreButton assetId={assetId} /> : undefined}
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data?.asset_state_store ?? []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="assets:assetStateStore.title"
        noRowsMessage={translate("assetStateStore.emptyState")}
        onStateChange={setTableURLState}
        showRowCountHeading={false}
        total={data?.total_entries ?? 0}
      />
    </>
  );
};
