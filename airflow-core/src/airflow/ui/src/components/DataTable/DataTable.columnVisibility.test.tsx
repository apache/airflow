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
import type { ColumnDef } from "@tanstack/react-table";
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper.tsx";

import { DataTable } from "./DataTable.tsx";
import type { DataTableFeatures } from "./features.ts";

const columns: Array<ColumnDef<DataTableFeatures, { name: string }>> = ["Name", "Added"].map((header) => ({
  cell: () => header,
  header,
  id: header,
}));

describe("DataTable column visibility", () => {
  it("applies the default visibility to columns missing from the stored visibility", () => {
    localStorage.setItem("dataTable:task:columnVisibility", JSON.stringify({ Name: true }));

    render(
      <DataTable
        columns={columns}
        data={[{ name: "John Doe" }]}
        initialState={{
          columnVisibility: { Added: false },
          pagination: { pageIndex: 0, pageSize: 10 },
          sorting: [],
        }}
        modelName="task"
        total={1}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByRole("columnheader", { name: "Name" })).toBeInTheDocument();
    expect(screen.queryByRole("columnheader", { name: "Added" })).toBeNull();
  });
});
