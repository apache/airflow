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
import { Box, Heading } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { PluginResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";

const columns: Array<ColumnDef<PluginResponse>> = [
  {
    accessorKey: "name",
    enableSorting: false,
    header: "Name",
  },
  {
    accessorKey: "source",
    enableSorting: false,
    header: "Source",
  },
];

export const Plugins = () => {
  const { data, error } = usePluginServiceGetPlugins();

  return (
    <Box p={2}>
      <Heading>Plugins</Heading>
      <DataTable
        columns={columns}
        data={data?.plugins ?? []}
        errorMessage={<ErrorAlert error={error} />}
        modelName="Plugin"
        total={data?.total_entries}
      />
    </Box>
  );
};
