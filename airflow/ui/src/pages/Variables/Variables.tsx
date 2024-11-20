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
import { Box } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";

import { useVariableServiceGetVariables } from "openapi/queries";
import type { VariableResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";

const variablesColumn = (): Array<ColumnDef<VariableResponse>> => [
  {
    accessorKey: "key",
    enableSorting: true,
    header: "Key",
    meta: {
      skeletonWidth: 20,
    },
  },
  {
    accessorKey: "value",
    enableSorting: true,
    header: "Value",
    meta: {
      skeletonWidth: 20,
    },
  },
  {
    accessorKey: "description",
    enableSorting: false,
    header: "Description",
    meta: {
      skeletonWidth: 60,
    },
  },
];

export const Variables = () => {
  const { setTableURLState, tableURLState } = useTableURLState({
    sorting: [{ desc: true, id: "key" }],
  });
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;

  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const {
    data,
    error: VariableError,
    isFetching,
    isLoading,
  } = useVariableServiceGetVariables({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  return (
    <Box>
      <ErrorAlert error={VariableError} />
      <DataTable
        columns={variablesColumn()}
        data={data ? data.variables : []}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Variable"
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
