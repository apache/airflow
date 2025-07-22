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
import { Box, Heading, HStack } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { PluginResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { ErrorAlert } from "src/components/ErrorAlert";

import { PluginImportErrors } from "./Dashboard/Stats/PluginImportErrors";

const createColumns = (translate: TFunction): Array<ColumnDef<PluginResponse>> => [
  {
    accessorKey: "name",
    enableSorting: false,
    header: translate("columns.name"),
  },
  {
    accessorKey: "source",
    enableSorting: false,
    header: translate("plugins.columns.source"),
  },
];

export const Plugins = () => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const { data, error } = usePluginServiceGetPlugins();

  const columns = useMemo(() => createColumns(translate), [translate]);

  return (
    <Box p={2}>
      <HStack>
        <Heading>{translate("common:admin.Plugins")}</Heading>
        <PluginImportErrors iconOnly />
      </HStack>
      <DataTable
        columns={columns}
        data={data?.plugins ?? []}
        errorMessage={<ErrorAlert error={error} />}
        modelName={translate("common:admin.Plugins")}
        total={data?.total_entries}
      />
    </Box>
  );
};
