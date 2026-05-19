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
import { Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";

import type { DAGRunResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import Time from "src/components/Time";

export const getBulkDagRunsColumns = (translate: TFunction): Array<ColumnDef<DAGRunResponse>> => [
  {
    accessorKey: "dag_run_id",
    cell: ({ row: { original } }) => <Text>{original.dag_run_id}</Text>,
    enableSorting: false,
    header: translate("dagRunId"),
  },
  {
    accessorKey: "state",
    cell: ({ row: { original } }) => (
      <StateBadge state={original.state}>{translate(`common:states.${original.state}`)}</StateBadge>
    ),
    enableSorting: false,
    header: translate("state"),
  },
  {
    accessorKey: "run_after",
    cell: ({ row: { original } }) => <Time datetime={original.run_after} />,
    enableSorting: false,
    header: translate("dagRun.runAfter"),
  },
];
