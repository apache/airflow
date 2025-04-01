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
import { Separator, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";

import type { ConfigOption, ConfigSection } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";

type ConfigPageSectionProp = {
  readonly section: ConfigSection;
};

const columns: Array<ColumnDef<ConfigOption>> = [
  {
    accessorKey: "key",
    enableSorting: false,
    header: () => "key",
  },
  {
    accessorKey: "value",
    enableSorting: false,
    header: () => "value",
  },
];

export const ConfigPageSection = ({ section }: ConfigPageSectionProp) => (
  <>
    <Text>{section.name}</Text>
    <DataTable columns={columns} data={section.options} />
    <Separator />
  </>
);
