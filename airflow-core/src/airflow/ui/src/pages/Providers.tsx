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

// TODO: Allow providers to define custom documentation URLs

import { Box, Heading, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { useProviderServiceGetProviders } from "openapi/queries";
import type { ProviderResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { urlRegex } from "src/constants/urlRegex";

const createColumns = (translate: TFunction): Array<ColumnDef<ProviderResponse>> => [
{
  accessorKey: "package_name",
  cell: ({ row: { original } }) => {
    const documentationUrl =
      original?.project_urls?.documentation ??
      `https://airflow.apache.org/docs/${original.package_name}/${original.version}/`;

    return (
      <Link
        aria-label={original.package_name}
        color="fg.info"
        href={documentationUrl}
        rel="noopener noreferrer"
        target="_blank"
      >
        {original.package_name}
      </Link>
    );
  },
  enableSorting: false,
  header: translate("providers.columns.packageName"),
},

  {
    accessorKey: "version",
    cell: ({ row: { original } }) => original.version,
    enableSorting: false,
    header: translate("providers.columns.version"),
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
    header: translate("columns.description"),
  },
];

export const Providers = () => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const { setTableURLState, tableURLState } = useTableURLState();

  const columns = useMemo(() => createColumns(translate), [translate]);

  const { pagination } = tableURLState;

  const { data, error } = useProviderServiceGetProviders({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
  });

  return (
    <Box p={2}>
      <Heading>{translate("common:admin.Providers")}</Heading>
      <DataTable
        columns={columns}
        data={data?.providers ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        modelName={translate("common:admin.Providers")}
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
