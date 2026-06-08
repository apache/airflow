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
import { Flex, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

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
