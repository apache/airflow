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

import type { AssetStateResponse } from "openapi/requests";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";
import { TruncatedText } from "src/components/TruncatedText";
import { useListAssetStates } from "src/queries/useAssetState";

import AddAssetStateButton from "./AddAssetStateButton";
import ClearAllAssetStateButton from "./ClearAllAssetStateButton";
import DeleteAssetStateButton from "./DeleteAssetStateButton";
import EditAssetStateButton from "./EditAssetStateButton";

type AssetStateTabProps = {
  readonly assetId: number;
};

type ColumnsProps = {
  readonly assetId: number;
  readonly translate: (key: string) => string;
};

const getColumns = ({ assetId, translate }: ColumnsProps): Array<ColumnDef<AssetStateResponse>> => [
  {
    accessorKey: "key",
    cell: ({ row: { original } }) => <Text>{original.key}</Text>,
    header: translate("key"),
  },
  {
    accessorKey: "value",
    cell: ({ row: { original } }) => {
      let parsed: unknown;

      try {
        parsed = JSON.parse(original.value);
      } catch {
        // not JSON — render as plain text
      }
      const isJsonObject = parsed !== null && parsed !== undefined && typeof parsed === "object";

      return isJsonObject ? (
        <RenderedJsonField collapsed content={parsed as object} enableClipboard={false} />
      ) : (
        <TruncatedText text={original.value} />
      );
    },
    enableSorting: false,
    header: translate("value"),
  },
  {
    accessorKey: "updated_at",
    cell: ({ row: { original } }) => <Time datetime={original.updated_at} />,
    header: translate("assetState.columns.updatedAt"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) => (
      <Flex justifyContent="end">
        <EditAssetStateButton assetId={assetId} stateKey={original.key} />
        <DeleteAssetStateButton assetId={assetId} stateKey={original.key} />
      </Flex>
    ),
    enableSorting: false,
    header: "",
  },
];

export const AssetStateTab = ({ assetId }: AssetStateTabProps) => {
  const { t: translate } = useTranslation("browse");
  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;

  const { data, error, isFetching, isLoading } = useListAssetStates({
    assetId,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
  });

  const columns = getColumns({ assetId, translate });

  return (
    <>
      <Flex gap={2} justifyContent="flex-end" mb={2}>
        <AddAssetStateButton assetId={assetId} />
        {(data?.total_entries ?? 0) > 0 ? <ClearAllAssetStateButton assetId={assetId} /> : undefined}
      </Flex>

      <ErrorAlert error={error} />
      <DataTable
        columns={columns}
        data={data?.asset_states ?? []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="browse:assetState.title"
        noRowsMessage={translate("assetState.emptyState")}
        onStateChange={setTableURLState}
        showRowCountHeading={false}
        total={data?.total_entries ?? 0}
      />
    </>
  );
};
