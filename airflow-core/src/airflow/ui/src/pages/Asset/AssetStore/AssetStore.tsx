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

import { useAssetStoreServiceListAssetStore } from "openapi/queries";
import type { AssetStoreResponse } from "openapi/requests";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StoreValueCell } from "src/components/StoreValueCell";
import Time from "src/components/Time";

import { AddAssetStoreButton } from "./AddAssetStoreButton";
import { ClearAllAssetStoreButton } from "./ClearAllAssetStoreButton";
import { DeleteAssetStoreButton } from "./DeleteAssetStoreButton";
import { EditAssetStoreButton } from "./EditAssetStoreButton";

type ColumnsProps = {
  readonly assetId: number;
  readonly translate: (key: string) => string;
};

const getColumns = ({ assetId, translate }: ColumnsProps): Array<ColumnDef<AssetStoreResponse>> => [
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
      if (
        writer.kind === "task" &&
        writer.dag_id !== null &&
        writer.run_id !== null &&
        writer.task_id !== null
      ) {
        const mapSuffix =
          writer.map_index !== null && writer.map_index !== undefined && writer.map_index >= 0
            ? `/mapped/${writer.map_index}`
            : "";
        const path = `/dags/${writer.dag_id}/runs/${writer.run_id}/tasks/${writer.task_id}${mapSuffix}`;

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

      return <Text>{writer.kind === "api" ? "API" : "Watcher"}</Text>;
    },
    enableSorting: false,
    header: translate("assets:assetStore.lastUpdatedBy"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditAssetStoreButton assetId={assetId} storeKey={original.key} />
        <DeleteAssetStoreButton assetId={assetId} storeKey={original.key} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
  },
];

export const AssetStore = () => {
  const { t: translate } = useTranslation(["assets", "common"]);
  const { assetId: rawAssetId } = useParams();
  const assetId = rawAssetId === undefined ? 0 : parseInt(rawAssetId, 10);
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;

  const { data, error, isFetching, isLoading } = useAssetStoreServiceListAssetStore({
    assetId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
  });

  const columns = getColumns({ assetId, translate });

  return (
    <>
      <Flex gap={2} justifyContent="flex-end" mb={2}>
        <AddAssetStoreButton assetId={assetId} />
        {(data?.total_entries ?? 0) > 0 ? <ClearAllAssetStoreButton assetId={assetId} /> : undefined}
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data?.asset_store ?? []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="assets:assetStore.title"
        noRowsMessage={translate("assetStore.emptyState")}
        onStateChange={setTableURLState}
        showRowCountHeading={false}
        total={data?.total_entries ?? 0}
      />
    </>
  );
};
