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
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper.tsx";

import { DataTable } from "./DataTable.tsx";
import type { DataTableFeatures } from "./features.ts";

const columns: Array<ColumnDef<DataTableFeatures, { name: string }>> = [
  {
    accessorKey: "name",
    cell: (info) => info.getValue(),
    header: "Name",
  },
  {
    cell: ({ row }) => (
      <button onClick={row.getToggleExpandedHandler()} type="button">
        {`expand ${row.original.name}`}
      </button>
    ),
    header: "Expand",
    id: "expand",
  },
];

const data = [{ name: "John Doe" }, { name: "Jane Doe" }];

const table = (rows: Array<{ name: string }>) => (
  <DataTable
    columns={columns}
    data={rows}
    getRowId={(row) => row.name}
    modelName="task"
    renderSubComponent={(row) => <Text>{`details of ${row.name}`}</Text>}
  />
);

describe("DataTable row expanding", () => {
  it("keeps an expanded row open, under the same record, when the data refetches in another order", async () => {
    const { rerender } = render(table(data), { wrapper: ChakraWrapper });

    expect(screen.queryByTestId("table-sub-row")).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "expand Jane Doe" }));
    expect(screen.getByTestId("table-sub-row")).toHaveTextContent("details of Jane Doe");

    rerender(table([...data].reverse()));
    // TanStack schedules its auto-reset of expanded rows in a microtask after the new data is read.
    await act(() => Promise.resolve());

    expect(screen.getAllByTestId("table-sub-row")).toHaveLength(1);
    expect(screen.getByTestId("table-sub-row")).toHaveTextContent("details of Jane Doe");
  });
});
