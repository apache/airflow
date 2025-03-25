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
import { Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { Link as RouterLink } from "react-router-dom";

import type { TaskInstanceResponse } from "openapi/requests/types.gen";
import { Tooltip } from "src/components/ui";
import { getTaskInstanceLink } from "src/utils/links";
import { trimText } from "src/utils/trimTextFn";

import { StateBadge } from "../StateBadge";

export const columns: Array<ColumnDef<TaskInstanceResponse>> = [
  {
    accessorKey: "task_display_name",
    cell: ({ row: { original } }) => (
      <Tooltip content={original.task_display_name}>
        <Link asChild color="fg.info" fontWeight="bold" maxWidth="200px" overflow="hidden">
          <RouterLink to={getTaskInstanceLink(original)}>
            {trimText(original.task_display_name, 25).trimmedText}
          </RouterLink>
        </Link>
      </Tooltip>
    ),
    enableSorting: false,
    header: "Task ID",
  },
  {
    accessorKey: "state",
    cell: ({
      row: {
        original: { state },
      },
    }) => <StateBadge state={state}>{state}</StateBadge>,
    enableSorting: false,
    header: () => "State",
  },
  {
    accessorKey: "rendered_map_index",
    enableSorting: false,
    header: "Map Index",
  },

  {
    accessorKey: "dag_run_id",
    enableSorting: false,
    header: "Run Id",
  },
];
