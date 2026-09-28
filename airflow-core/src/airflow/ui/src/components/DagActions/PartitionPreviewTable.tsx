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
import { useState } from "react";

import { Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";

import type { DryRunBackfillResponse } from "openapi/requests/types.gen";

import { DataTable } from "src/components/DataTable";

const pageSize = 10;

type PartitionRow = { partition_key: string } & DryRunBackfillResponse;

type PartitionPreviewTableProps = {
  readonly backfills: Array<PartitionRow>;
};

const getColumns = (translate: TFunction): Array<ColumnDef<PartitionRow>> => [
  {
    accessorKey: "partition_key",
    cell: ({ row }) =>
      row.original.partition_key === "" ? (
        <Text color="fg.muted">—</Text>
      ) : (
        <Text>{row.original.partition_key}</Text>
      ),
    enableSorting: false,
    header: translate("dagRun.partitionKey"),
  },
];

export const PartitionPreviewTable = ({ backfills }: PartitionPreviewTableProps) => {
  const { t: translate } = useTranslation(["common"]);
  const [pageIndex, setPageIndex] = useState(0);

  const columns = getColumns(translate);

  return (
    <DataTable
      columns={columns}
      data={backfills.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize)}
      hideRowCountHeading
      initialState={{ pagination: { pageIndex, pageSize }, sorting: [] }}
      modelName="partition"
      onStateChange={(state) => setPageIndex(state.pagination.pageIndex)}
      total={backfills.length}
    />
  );
};
