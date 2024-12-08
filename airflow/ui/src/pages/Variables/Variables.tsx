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

const columns: Array<ColumnDef<VariableResponse>> = [
  {
    accessorKey: "key",
    header: "Key",
    meta: {
      skeletonWidth: 20,
    },
  },
  {
    accessorKey: "value",
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
      skeletonWidth: 50,
    },
  },
];

export const Variables = () => {
  const { setTableURLState, tableURLState } = useTableURLState({
    sorting: [{ desc: false, id: "key" }],
  });
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort
    ? `${sort.desc ? "-" : ""}${sort.id === "value" ? "_val" : sort.id}`
    : undefined;

  const { data, error, isFetching, isLoading } = useVariableServiceGetVariables(
    {
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
  );

  return (
    <Box>
      <DataTable
        columns={columns}
        data={data ? data.variables : []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Variables"
        onStateChange={setTableURLState}
        total={data ? data.total_entries : 0}
      />
    </Box>
  );
};
