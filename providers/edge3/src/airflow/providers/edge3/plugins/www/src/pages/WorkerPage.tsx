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
    enableSorting: false,  // Sorting is a future improvement
    header: "Worker Name",
  },
  {
    accessorKey: "state",
    enableSorting: false,  // Sorting is a future improvement
    header: "State",
  },
];

export const WorkerPage = () => {
  const { data, error } = useUiServiceWorker();

  /* DataTable is somehow broken, always shows:
  `can't access property "CreatePortal", ie is undefined`
  -..therefore start with a very ugly plain table...

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
  */
  if (data)
    return (
      <Box p={2}>
        <table cellPadding={2} cellSpacing={2} style={{ border: "1px solid gray" }}>
          <thead>
            <tr>
              <th style={{ border: "1px solid gray", padding: "2px" }}>Worker Name</th>
              <th style={{ border: "1px solid gray", padding: "2px" }}>State</th>
            </tr>
          </thead>
          <tbody>
            {data.workers.map((worker) => (
              <tr key={worker.worker_name}>
                <td style={{ border: "1px solid gray", padding: "2px" }}>{worker.worker_name}</td>
                <td style={{ border: "1px solid gray", padding: "2px" }}>{worker.state}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Box>
    );
  if (error) {
    return (
      <Box p={2}>
        <p>Unable to load data:</p>
        <ErrorAlert error={error} />
      </Box>
    );
  }
  return (<Box p={2}>Loading...</Box>);
};
