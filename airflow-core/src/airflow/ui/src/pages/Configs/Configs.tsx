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
import { Heading, Separator } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";

import { useConfigServiceGetConfig } from "openapi/queries";
import type { ConfigOption } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";

type ConfigColums = {
  section: string;
} & ConfigOption;

const columns: Array<ColumnDef<ConfigColums>> = [
  {
    accessorKey: "section",
    enableSorting: false,
    header: "Section",
  },
  {
    accessorKey: "key",
    enableSorting: false,
    header: "Key",
  },
  {
    accessorKey: "value",
    enableSorting: false,
    header: "Value",
  },
];

export const Configs = () => {
  const { data, error } = useConfigServiceGetConfig();

  const render =
    data?.sections.flatMap((section) =>
      section.options.map((option) => ({
        ...option,
        section: section.name,
      })),
    ) ?? [];

  return (
    <>
      <Heading mb={4}>Airflow Configuration</Heading>
      <Separator />
      {error === null ? (
        <DataTable columns={columns} data={render} modelName="Config" />
      ) : (
        <ErrorAlert error={error} />
      )}
    </>
  );
};
