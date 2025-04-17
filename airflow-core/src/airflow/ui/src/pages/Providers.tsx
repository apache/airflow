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
import { Box, Heading, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";

import { useProviderServiceGetProviders } from "openapi/queries";
import type { ProviderResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";
import { urlRegex } from "src/constants/urlRegex";

const columns: Array<ColumnDef<ProviderResponse>> = [
  {
    accessorKey: "package_name",
    cell: ({ row: { original } }) => (
      <Link
        aria-label={original.package_name}
        color="fg.info"
        href={`https://airflow.apache.org/docs/${original.package_name}/${original.version}/`}
        rel="noopener noreferrer"
        target="_blank"
      >
        {original.package_name}
      </Link>
    ),
    enableSorting: false,
    header: "Package Name",
  },
  {
    accessorKey: "version",
    cell: ({ row: { original } }) => original.version,
    enableSorting: false,
    header: () => "Version",
  },
  {
    accessorKey: "description",
    cell: ({ row: { original } }) => {
      const urls = original.description.match(urlRegex);
      const cleanText = original.description.replaceAll(/\n(?:and)?/gu, " ").split(" ");

      return cleanText.map((part) =>
        urls?.includes(part) ? (
          <Link color="fg.info" href={part} key={part} rel="noopener noreferrer" target="_blank">
            {part}
          </Link>
        ) : (
          `${part} `
        ),
      );
    },
    enableSorting: false,
    header: "Description",
  },
];

export const Providers = () => {
  const { data, error } = useProviderServiceGetProviders();

  return (
    <Box p={2}>
      <Heading>Providers</Heading>
      <DataTable
        columns={columns}
        data={data?.providers ?? []}
        errorMessage={<ErrorAlert error={error} />}
        modelName="Provider"
        total={data?.total_entries}
      />
    </Box>
  );
};
