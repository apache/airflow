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
import { ColumnDef } from "@tanstack/react-table";

import { useUiServiceWorker } from "openapi/queries";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";
import type { Worker } from "openapi/requests/types.gen";

const createColumns = (): Array<ColumnDef<Worker>> => [
  {
    accessorKey: "worker_name",
    enableSorting: true,
    header: "Worker Name",
  },
  {
    accessorKey: "state",
    enableSorting: true,
    header: "State",
  },
];

export const WorkerPage = () => {
  const { data, error} = useUiServiceWorker();

  return (
    <Box p={2}>
      <DataTable
        columns={createColumns()}
        data={data?.workers ?? []}
        errorMessage={<ErrorAlert error={error} />}
        modelName={"Edge Worker"}
        total={data?.total_entries}
      />
    </Box>
  );
};
