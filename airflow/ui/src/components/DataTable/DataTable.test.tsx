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
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { DataTable } from "./DataTable.tsx";

const columns: Array<ColumnDef<{ name: string }>> = [
  {
    accessorKey: "name",
    cell: (info) => info.getValue(),
    header: "Name",
  },
];

const data = [{ name: "John Doe" }, { name: "Jane Doe" }];

const pagination: PaginationState = { pageIndex: 0, pageSize: 1 };
const onStateChange = vi.fn();

describe("DataTable", () => {
  it("renders table with data", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        onStateChange={onStateChange}
        total={2}
      />,
    );

    expect(screen.getByText("John Doe")).toBeInTheDocument();
    expect(screen.getByText("Jane Doe")).toBeInTheDocument();
  });

  it("disables previous page button on first page", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        onStateChange={onStateChange}
        total={2}
      />,
    );

    expect(screen.getByText("<<")).toBeDisabled();
    expect(screen.getByText("<")).toBeDisabled();
  });

  it("disables next button when on last page", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{
          pagination: { pageIndex: 1, pageSize: 10 },
          sorting: [],
        }}
        onStateChange={onStateChange}
        total={2}
      />,
    );

    expect(screen.getByText(">>")).toBeDisabled();
    expect(screen.getByText(">")).toBeDisabled();
  });
});
