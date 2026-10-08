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
import type { ColumnDef, PaginationState } from "@tanstack/react-table";
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper.tsx";

import { DataTable } from "./DataTable.tsx";
import type { DataTableFeatures } from "./features.ts";

const columns: Array<ColumnDef<DataTableFeatures, { name: string }>> = [
  {
    accessorKey: "name",
    cell: (info) => info.getValue(),
    header: "Name",
  },
];

const data = [{ name: "John Doe" }, { name: "Jane Doe" }];

const pagination: PaginationState = { pageIndex: 0, pageSize: 1 };

describe("DataTable onStateChange", () => {
  it.each([
    { enableMultiSort: undefined, expected: [{ desc: false, id: "Second" }] },
    {
      enableMultiSort: true,
      expected: [
        { desc: false, id: "name" },
        { desc: false, id: "Second" },
      ],
    },
  ])(
    "shift-click adds a secondary sort only when enableMultiSort=$enableMultiSort",
    ({ enableMultiSort, expected }) => {
      const sortOnStateChange = vi.fn();

      render(
        <DataTable
          columns={[...columns, { accessorKey: "name", header: "Second", id: "Second" }]}
          data={data}
          enableMultiSort={enableMultiSort}
          initialState={{ pagination, sorting: [{ desc: false, id: "name" }] }}
          modelName="task"
          onStateChange={sortOnStateChange}
          total={2}
        />,
        { wrapper: ChakraWrapper },
      );

      fireEvent.click(screen.getByText("Second", { selector: "button" }), { shiftKey: true });

      expect(sortOnStateChange).toHaveBeenLastCalledWith(expect.objectContaining({ sorting: expected }));
    },
  );

  it("reports the next page alongside the current sorting when paging forward", async () => {
    const pageOnStateChange = vi.fn();

    render(
      <DataTable
        columns={columns}
        data={[{ name: "John Doe" }]}
        initialState={{ pagination, sorting: [{ desc: true, id: "name" }] }}
        modelName="task"
        onStateChange={pageOnStateChange}
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    fireEvent.click(screen.getByTestId("next"));

    await waitFor(() =>
      expect(pageOnStateChange).toHaveBeenLastCalledWith(
        expect.objectContaining({
          pagination: { pageIndex: 1, pageSize: 1 },
          sorting: [{ desc: true, id: "name" }],
        }),
      ),
    );
  });
});
