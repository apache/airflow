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
import type { ColumnDef } from "@tanstack/react-table";
import { useParams } from "react-router-dom";

import { useAssetServiceGetAssetEvents } from "openapi/queries";
import type { AssetEventResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

type AssetEventRow = { row: { original: AssetEventResponse } };

const assetEventColumns = (assetId?: string): Array<ColumnDef<AssetEventResponse>> => [
  ...(Boolean(assetId)
    ? []
    : [
        {
          accessorKey: "name",
          enableSorting: false,
          header: "Asset",
        },
      ]),
  {
    accessorKey: "timestamp",
    cell: ({ row: { original } }: AssetEventRow) => <Time datetime={original.timestamp} />,
    header: "Timestamp",
  },
  {
    accessorKey: "source_run_id",
    enableSorting: false,
    header: "Source",
  },
  {
    accessorKey: "created_dagruns",
    cell: ({ row: { original } }: AssetEventRow) => original.created_dagruns.length,
    enableSorting: false,
    header: "Created Dag Runs",
  },
];

export const AssetEvents = () => {
  const { assetId } = useParams();

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "-timestamp";

  const { data, error, isLoading } = useAssetServiceGetAssetEvents({
    assetId: assetId === undefined ? undefined : parseInt(assetId, 10),
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  return (
    <DataTable
      columns={assetEventColumns(assetId)}
      data={data?.asset_events ?? []}
      errorMessage={<ErrorAlert error={error} />}
      initialState={tableURLState}
      isLoading={isLoading}
      modelName="Asset Event"
      onStateChange={setTableURLState}
      total={data?.total_entries}
    />
  );
};
