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
import { Box, Text } from "@chakra-ui/react";
import { Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { Link as RouterLink } from "react-router-dom";

import type { TaskInstanceCollectionResponse, TaskInstanceResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { Status, Tooltip } from "src/components/ui";
import { getTaskInstanceLink } from "src/utils/links";
import { trimText } from "src/utils/trimTextFn";

import { Accordion } from "../ui";

const columns: Array<ColumnDef<TaskInstanceResponse>> = [
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
    }) => <Status state={state}>{state}</Status>,
    enableSorting: false,
    header: () => "State",
  },
  {
    accessorFn: (row: TaskInstanceResponse) => row.rendered_map_index ?? row.map_index,
    enableSorting: false,
    header: "Map Index",
  },

  {
    accessorKey: "dag_run_id",
    enableSorting: false,
    header: "Run Id",
  },
];

type Props = {
  readonly affectedTasks?: TaskInstanceCollectionResponse;
};

// Table is in memory, pagination and sorting are disabled.
// TODO: Make a front-end only unconnected table component with client side ordering and pagination
const ClearRunTasksAccordion = ({ affectedTasks }: Props) => (
  <Accordion.Root collapsible variant="enclosed">
    <Accordion.Item key="tasks" value="tasks">
      <Accordion.ItemTrigger>
        <Text fontWeight="bold">Affected Tasks: {affectedTasks?.total_entries ?? 0}</Text>
      </Accordion.ItemTrigger>
      <Accordion.ItemContent>
        <Box maxH="400px" overflowY="scroll">
          <DataTable
            columns={columns}
            data={affectedTasks?.task_instances ?? []}
            displayMode="table"
            initialState={{
              pagination: {
                pageIndex: 0,
                pageSize: affectedTasks?.total_entries ?? 0,
              },
              sorting: [],
            }}
            modelName="Task Instance"
            total={affectedTasks?.total_entries}
          />
        </Box>
      </Accordion.ItemContent>
    </Accordion.Item>
  </Accordion.Root>
);

export default ClearRunTasksAccordion;
