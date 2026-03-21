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
import type { ColumnDef, PaginationState } from "@tanstack/react-table";
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { ChakraWrapper } from "src/utils/ChakraWrapper.tsx";

import { DataTable } from "./DataTable.tsx";
import type { CardDef } from "./types.ts";

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

const cardDef: CardDef<{ name: string }> = {
  card: ({ row }) => <Text>My name is {row.name}.</Text>,
};

describe("DataTable", () => {
  it("renders table with data", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        onStateChange={onStateChange}
        total={2}
      />,
      {
        wrapper: ChakraWrapper,
      },
    );

    expect(screen.getByText("John Doe")).toBeInTheDocument();
    expect(screen.getByText("Jane Doe")).toBeInTheDocument();
  });

  it("disables previous page button on first page", () => {
    render(
      <DataTable
        columns={columns}
        data={[{ name: "John Doe" }]}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        onStateChange={onStateChange}
        total={2}
      />,
      {
        wrapper: ChakraWrapper,
      },
    );

    expect(screen.getByTestId("prev")).toBeDisabled();
  });

  it("disables next button when on last page", () => {
    render(
      <DataTable
        columns={columns}
        data={[{ name: "John Doe" }]}
        initialState={{
          pagination: { pageIndex: 0, pageSize: 10 },
          sorting: [],
        }}
        modelName="task"
        onStateChange={onStateChange}
        total={2}
      />,
      {
        wrapper: ChakraWrapper,
      },
    );

    expect(screen.getByTestId("next")).toBeDisabled();
  });

  it("renders no pagination when not needed", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        onStateChange={onStateChange}
        total={2}
      />,
      {
        wrapper: ChakraWrapper,
      },
    );

    expect(screen.queryByTestId("prev")).toBeNull();
    expect(screen.queryByTestId("next")).toBeNull();
  });

  it("when isLoading renders skeleton columns", () => {
    render(<DataTable columns={columns} data={data} isLoading modelName="task" />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getAllByTestId("skeleton")).toHaveLength(10);
  });

  it("still displays table if mode is card but there is no cardDef", () => {
    render(<DataTable columns={columns} data={data} displayMode="card" modelName="task" />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByText("Name")).toBeInTheDocument();
  });

  it("displays cards if mode is card and there is cardDef", () => {
    render(
      <DataTable cardDef={cardDef} columns={columns} data={data} displayMode="card" modelName="task" />,
      {
        wrapper: ChakraWrapper,
      },
    );

    expect(screen.getByText("My name is John Doe.")).toBeInTheDocument();
  });

  it("displays skeleton for loading card list", () => {
    render(
      <DataTable
        cardDef={cardDef}
        columns={columns}
        data={data}
        displayMode="card"
        isLoading
        modelName="task"
        skeletonCount={5}
      />,
      {
        wrapper: ChakraWrapper,
      },
    );

    expect(screen.getAllByTestId("skeleton")).toHaveLength(5);
  });

  it("renders row count heading by default when total > 0", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByRole("heading")).toHaveTextContent("2 task");
  });

  it("does not render row count heading when showRowCountHeading is false", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        showRowCountHeading={false}
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.queryByRole("heading")).toBeNull();
  });

  it("uses translated zero-count model name in empty state", () => {
    render(
      <DataTable
        columns={columns}
        data={[]}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        total={0}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByText(/noitemsFound/iu)).toBeInTheDocument();
  });

  it("renders display toggle when showDisplayToggle and onDisplayToggleChange are provided", () => {
    const handleToggle = vi.fn();

    render(
      <DataTable
        columns={columns}
        data={data}
        displayMode="table"
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        onDisplayToggleChange={handleToggle}
        showDisplayToggle
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByLabelText(/toggleTableView/iu)).toBeInTheDocument();
  });

  it("does not render display toggle without showDisplayToggle", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        displayMode="table"
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.queryByLabelText(/toggleTableView/iu)).toBeNull();
  });
});
