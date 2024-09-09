import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { DataTable } from "./DataTable.tsx";
import { ColumnDef, PaginationState } from "@tanstack/react-table";

const columns: ColumnDef<any>[] = [
  {
    accessorKey: "name",
    header: "Name",
    cell: (info) => info.getValue(),
  },
];

const data = [{ name: "John Doe" }, { name: "Jane Doe" }];

const pagination: PaginationState = { pageIndex: 0, pageSize: 1 };
const setPagination = vi.fn();

describe("DataTable", () => {
  it("renders table with data", () => {
    render(
      <DataTable
        data={data}
        total={2}
        columns={columns}
        pagination={pagination}
        setPagination={setPagination}
      />
    );

    expect(screen.getByText("John Doe")).toBeInTheDocument();
    expect(screen.getByText("Jane Doe")).toBeInTheDocument();
  });

  it("disables previous page button on first page", () => {
    render(
      <DataTable
        data={data}
        total={2}
        columns={columns}
        pagination={pagination}
        setPagination={setPagination}
      />
    );

    expect(screen.getByText("<<")).toBeDisabled();
    expect(screen.getByText("<")).toBeDisabled();
  });

  it("disables next button when on last page", () => {
    render(
      <DataTable
        data={data}
        total={2}
        columns={columns}
        pagination={{ pageIndex: 1, pageSize: 10 }}
        setPagination={setPagination}
      />
    );

    expect(screen.getByText(">>")).toBeDisabled();
    expect(screen.getByText(">")).toBeDisabled();
  });
});
