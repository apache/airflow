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
import { Box, Editable, Text, VStack } from "@chakra-ui/react";
import { Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { ChangeEvent } from "react";
import { Link as RouterLink } from "react-router-dom";

import type {
  DAGRunResponse,
  TaskInstanceCollectionResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import ReactMarkdown from "src/components/ReactMarkdown";
import { Status, Tooltip } from "src/components/ui";
import { Accordion } from "src/components/ui";
import { getTaskInstanceLink } from "src/utils/links";
import { trimText } from "src/utils/trimTextFn";

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
  readonly note: DAGRunResponse["note"];
  readonly setNote: (value: string) => void;
};

// Table is in memory, pagination and sorting are disabled.
// TODO: Make a front-end only unconnected table component with client side ordering and pagination
const ClearRunTasksAccordion = ({ affectedTasks, note, setNote }: Props) => (
  <Accordion.Root collapsible defaultValue={["note"]} multiple={false} variant="enclosed">
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
    <Accordion.Item key="note" value="note">
      <Accordion.ItemTrigger>
        <Text fontWeight="bold">Note</Text>
      </Accordion.ItemTrigger>
      <Accordion.ItemContent>
        <Editable.Root
          onChange={(event: ChangeEvent<HTMLInputElement>) => setNote(event.target.value)}
          value={note ?? ""}
        >
          <Editable.Preview
            _hover={{ backgroundColor: "transparent" }}
            alignItems="flex-start"
            as={VStack}
            gap="0"
            height="200px"
            overflowY="auto"
            width="100%"
          >
            {Boolean(note) ? (
              <ReactMarkdown>{note}</ReactMarkdown>
            ) : (
              <Text color="gray" opacity={0.6}>
                Add a note...
              </Text>
            )}
          </Editable.Preview>
          <Editable.Textarea
            data-testid="notes-input"
            height="200px"
            overflowY="auto"
            placeholder="Add a note..."
            resize="none"
          />
        </Editable.Root>
      </Accordion.ItemContent>
    </Accordion.Item>
  </Accordion.Root>
);

export default ClearRunTasksAccordion;
