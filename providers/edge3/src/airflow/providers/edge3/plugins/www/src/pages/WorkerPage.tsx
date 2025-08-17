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

import { DataTable } from "src/components/DataTable";
// import { ErrorAlert } from "src/components/ErrorAlert";

/** Mockup until real backend start */
type WorkerMockResponse = {
    hostname: string;
    state: string;
    queues: Array<(string)>;
    firstOnline: string;
    lastHeartbeat: string;
    activeJobs: number;
    systemInformation: Array<(string)>;
    maintenanceComment: string | null;
};
/** Mockup until real backend end */

const createColumns = (): Array<ColumnDef<WorkerMockResponse>> => [
  {
    accessorKey: "hostname",
    enableSorting: true,
    header: "Hostname",
  },
  {
    accessorKey: "state",
    enableSorting: true,
    header: "State",
  },
];

export const WorkerPage = () => {
  const data = null;

  return (
    <Box p={2}>
      <DataTable
        columns={createColumns()}
        data={data?.workers ?? []}
        // errorMessage={<ErrorAlert error={error} />}
        modelName={"Edge Worker"}
        total={data?.total_entries}
      />
    </Box>
  );
};
