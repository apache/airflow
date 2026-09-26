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
import { type ColumnDef, getCoreRowModel, useReactTable } from "@tanstack/react-table";
import "@testing-library/jest-dom";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper.tsx";

import { TableList } from "./TableList.tsx";

const columns: Array<ColumnDef<{ name: string }>> = [{ accessorKey: "name", header: "Name" }];
const data = [{ name: "John Doe" }];

const Harness = ({ enableMultiSort }: { readonly enableMultiSort: boolean }) => {
  const table = useReactTable({ columns, data, enableMultiSort, getCoreRowModel: getCoreRowModel() });

  return <TableList table={table} />;
};

describe("TableList", () => {
  it.each([
    { enableMultiSort: false, expected: null },
    { enableMultiSort: true, expected: "sortMultiColumnHint" },
  ])(
    "explains the shift hotkey in a header tooltip only when enableMultiSort=$enableMultiSort",
    async ({ enableMultiSort, expected }) => {
      vi.useFakeTimers();

      render(<Harness enableMultiSort={enableMultiSort} />, { wrapper: ChakraWrapper });

      const trigger = screen.getByText("Name", { selector: "button" }).closest('[data-part="trigger"]');

      try {
        if (expected === null) {
          expect(trigger).toBeNull();
        } else {
          await act(async () => {
            fireEvent.focus(trigger as Element);
            fireEvent.pointerEnter(trigger as Element);
            await vi.advanceTimersByTimeAsync(500);
          });

          expect(screen.getByText(expected)).toBeInTheDocument();
        }
      } finally {
        vi.useRealTimers();
      }
    },
  );
});
