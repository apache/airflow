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

import {
  ColumnDef,
  Row,
  OnChangeFn,
  PaginationState,
} from "@tanstack/react-table";
import { MdExpandMore } from "react-icons/md";
import { Box, Code } from "@chakra-ui/react";

import { DAG } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable.tsx";

const columns: ColumnDef<DAG>[] = [
  {
    id: "expander",
    header: () => null,
    cell: ({ row }) => {
      return row.getCanExpand() ? (
        <button
          {...{
            onClick: row.getToggleExpandedHandler(),
            style: { cursor: "pointer" },
          }}
        >
          <Box
            transform={row.getIsExpanded() ? "rotate(-180deg)" : "none"}
            transition="transform 0.2s"
          >
            <MdExpandMore />
          </Box>
        </button>
      ) : null;
    },
  },
  {
    accessorKey: "dag_display_name",
    header: "DAG",
  },
  {
    accessorKey: "is_paused",
    header: () => "Is Paused",
  },
  {
    accessorKey: "timetable_description",
    header: () => "Timetable",
  },
  {
    accessorKey: "description",
    header: () => "Description",
  },
];

const renderSubComponent = ({ row }: { row: Row<DAG> }) => {
  return (
    <pre style={{ fontSize: "10px" }}>
      <Code>{JSON.stringify(row.original, null, 2)}</Code>
    </pre>
  );
};

export const DagsList = ({
  data,
  total,
  pagination,
  setPagination,
}: {
  data: DAG[];
  total: number | undefined;
  pagination: PaginationState;
  setPagination: OnChangeFn<PaginationState>;
}) => {
  return (
    <DataTable
      data={data}
      total={total}
      columns={columns}
      getRowCanExpand={() => true}
      renderSubComponent={renderSubComponent}
      pagination={pagination}
      setPagination={setPagination}
    />
  );
};
