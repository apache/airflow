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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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

// The columns menu is only shown by default once a table has many columns
const wideColumns: Array<ColumnDef<{ name: string }>> = [
  ...columns,
  ...["Second", "Third", "Fourth", "Fifth", "Sixth"].map((header) => ({
    cell: () => header,
    header,
    id: header,
  })),
];

const columnsMenuLabel = "table.filterColumns";

const data = [{ name: "John Doe" }, { name: "Jane Doe" }];

const pagination: PaginationState = { pageIndex: 0, pageSize: 1 };
const onStateChange = vi.fn();

const cardDef: CardDef<{ name: string }> = {
  card: ({ row }) => <Text>My name is {row.name}.</Text>,
};

/** Header row buttons in DOM order, named by accessible label, for asserting slot placement. */
const headerButtonOrder = () =>
  within(screen.getByTestId("data-table-header"))
    .getAllByRole("button")
    .map((button) => button.getAttribute("aria-label") ?? button.textContent);

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

  it("renders no offset pagination when no total is provided", () => {
    render(
      <DataTable columns={columns} data={data} initialState={{ pagination, sorting: [] }} modelName="task" />,
      { wrapper: ChakraWrapper },
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

  it("renders table skeletons when card mode falls back to a table", () => {
    render(<DataTable columns={columns} data={data} displayMode="card" isLoading modelName="task" />, {
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

  it("renders the row count heading by default", () => {
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

  it("renders row count heading for cursor pagination when total is provided", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        nextCursor="next"
        onStateChange={onStateChange}
        total={2}
        totalEntriesLimit={50_000}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByRole("heading")).toHaveTextContent("2 task");
  });

  it("groups thousands in the row count heading", () => {
    render(<DataTable columns={columns} data={data} modelName="task" total={1234} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByRole("heading")).toHaveTextContent("1,234 task");
  });

  it("renders a zero row count heading when there are no records", () => {
    render(<DataTable columns={columns} data={[]} modelName="task" total={0} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByRole("heading")).toHaveTextContent("0 task");
  });

  // i18next is not initialised here, so keys come back raw. The `_other` suffix is the point: the
  // count-free label must read that key directly rather than passing a stand-in count.
  it("names the model without a count when no total is provided", () => {
    render(<DataTable columns={columns} data={data} modelName="task" />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByRole("heading")).toHaveTextContent(/^task_other$/u);
  });

  it("keeps the row count heading while refetching", () => {
    render(<DataTable columns={columns} data={data} isFetching modelName="task" total={2} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByRole("heading")).toHaveTextContent("2 task");
  });

  it("renders a capped row count heading when total reaches totalEntriesLimit", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        nextCursor="next"
        onStateChange={onStateChange}
        total={50_000}
        totalEntriesLimit={50_000}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByRole("heading")).toHaveTextContent("50,000+ task");
  });

  it("keeps the row count heading during the initial load", () => {
    render(<DataTable columns={columns} data={[]} isLoading modelName="task" total={0} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByRole("heading")).toHaveTextContent(/^task_other$/u);
  });

  it("does not render row count heading when hideRowCountHeading is set", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        hideRowCountHeading
        initialState={{ pagination, sorting: [] }}
        modelName="task"
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

  it("keeps column headers and spans all columns for the empty message in table display", () => {
    render(
      <DataTable columns={wideColumns} data={[]} modelName="task" noRowsMessage="nothing here" total={0} />,
      { wrapper: ChakraWrapper },
    );

    const table = screen.getByTestId("table-list");

    expect(within(table).getByText("Name")).toBeInTheDocument();
    expect(within(table).getByText("Sixth")).toBeInTheDocument();

    const cell = within(screen.getByTestId("table-no-rows")).getByText("nothing here");

    expect(cell.closest("td")).toHaveAttribute("colspan", String(wideColumns.length));
  });

  it("renders the empty message in card display", () => {
    render(
      <DataTable
        cardDef={cardDef}
        columns={columns}
        data={[]}
        displayMode="card"
        modelName="task"
        noRowsMessage="nothing here"
        total={0}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(within(screen.getByTestId("card-no-rows")).getByText("nothing here")).toBeInTheDocument();
    expect(screen.queryByTestId("table-list")).toBeNull();
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

  it("renders columns menu in the header row rather than inside the table", () => {
    render(<DataTable columns={wideColumns} data={data} modelName="task" total={2} />, {
      wrapper: ChakraWrapper,
    });

    expect(
      within(screen.getByTestId("data-table-header")).getByLabelText(columnsMenuLabel),
    ).toBeInTheDocument();
    expect(within(screen.getByTestId("table-list")).queryByLabelText(columnsMenuLabel)).toBeNull();
  });

  it("does not render columns menu in card display", () => {
    render(
      <DataTable
        cardDef={cardDef}
        columns={wideColumns}
        data={data}
        displayMode="card"
        modelName="task"
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.queryByLabelText(columnsMenuLabel)).toBeNull();
  });

  it("keeps columns menu available when there are no rows", () => {
    render(<DataTable columns={wideColumns} data={[]} modelName="task" total={0} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByLabelText(columnsMenuLabel)).toBeInTheDocument();
    expect(screen.getByText(/noitemsFound/iu)).toBeInTheDocument();
  });

  it("hides columns menu when showColumnsMenu is false", () => {
    render(
      <DataTable columns={wideColumns} data={data} modelName="task" showColumnsMenu={false} total={2} />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.queryByLabelText(columnsMenuLabel)).toBeNull();
  });

  it("shows columns menu for few columns when showColumnsMenu is true", () => {
    render(<DataTable columns={columns} data={data} modelName="task" showColumnsMenu total={2} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.getByLabelText(columnsMenuLabel)).toBeInTheDocument();
  });

  it("hides a column when unchecked in the columns menu", async () => {
    render(<DataTable columns={wideColumns} data={data} modelName="task" total={2} />, {
      wrapper: ChakraWrapper,
    });

    const trigger = screen.getByLabelText(columnsMenuLabel);

    fireEvent.pointerDown(trigger);
    fireEvent.click(trigger);

    const menuItem = await waitFor(() => screen.getByRole("menuitem", { name: "Second" }));

    fireEvent.click(menuItem);

    await waitFor(() => expect(within(screen.getByTestId("table-list")).queryByText("Second")).toBeNull());
  });

  // Each slot needs its own entry in the header row condition, or its content vanishes whenever
  // nothing else occupies the row — as it does on tables that hide the row count heading.
  const actionSlots = [
    ["filterActions", { filterActions: <button type="button">slot content</button> }],
    ["presentationActions", { presentationActions: <button type="button">slot content</button> }],
    ["primaryActions", { primaryActions: <button type="button">slot content</button> }],
  ] as const;

  it.each(actionSlots)("renders %s with no rows and no heading", (_name, slot) => {
    render(
      <DataTable columns={columns} data={[]} hideRowCountHeading modelName="task" total={0} {...slot} />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.queryByRole("heading")).toBeNull();
    expect(screen.getByText("slot content")).toBeInTheDocument();
  });

  it("renders all three action slots together", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        filterActions={<button type="button">filter slot</button>}
        modelName="task"
        presentationActions={<button type="button">presentation slot</button>}
        primaryActions={<button type="button">primary slot</button>}
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    const header = within(screen.getByTestId("data-table-header"));

    expect(header.getByText("filter slot")).toBeInTheDocument();
    expect(header.getByText("presentation slot")).toBeInTheDocument();
    expect(header.getByText("primary slot")).toBeInTheDocument();
  });

  it("renders presentation actions before the columns menu", () => {
    render(
      <DataTable
        columns={wideColumns}
        data={data}
        modelName="task"
        presentationActions={<button type="button">custom action</button>}
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    const order = headerButtonOrder();

    expect(order.indexOf("custom action")).toBeLessThan(order.indexOf(columnsMenuLabel));
  });

  it("renders primary actions before presentation actions", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        modelName="task"
        presentationActions={<button type="button">presentation slot</button>}
        primaryActions={<button type="button">primary slot</button>}
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    const order = headerButtonOrder();

    expect(order.indexOf("primary slot")).toBeLessThan(order.indexOf("presentation slot"));
  });

  it("renders headingExtra next to the row count heading", () => {
    render(
      <DataTable
        columns={columns}
        data={data}
        headingExtra={<Text>heading extra</Text>}
        initialState={{ pagination, sorting: [] }}
        modelName="task"
        total={2}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.getByRole("heading")).toHaveTextContent("2 task");
    expect(screen.getByText("heading extra")).toBeInTheDocument();
  });

  it("renders headingExtra when the row count heading is hidden", () => {
    render(
      <DataTable
        columns={columns}
        data={[]}
        headingExtra={<Text>heading extra</Text>}
        hideRowCountHeading
        modelName="task"
        total={0}
      />,
      { wrapper: ChakraWrapper },
    );

    expect(screen.queryByRole("heading")).toBeNull();
    expect(screen.getByText("heading extra")).toBeInTheDocument();
  });

  it("renders no header row when there is nothing to show", () => {
    render(<DataTable columns={columns} data={data} hideRowCountHeading modelName="task" total={2} />, {
      wrapper: ChakraWrapper,
    });

    expect(screen.queryByTestId("data-table-header")).toBeNull();
  });
});
