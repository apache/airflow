import { render, screen, fireEvent, waitFor, within } from "@testing-library/react";
import { MemoryRouter, Route, Routes, useSearchParams } from "react-router-dom";
import { describe, it, expect, vi } from "vitest";

import type { TaskResponse } from "openapi/requests/types.gen";
import TaskFilters from "src/pages/Dag/Tasks/TaskFilters/TaskFilters";
import { ChakraWrapper } from "src/utils/ChakraWrapper";

/** ================= Mock src/components/ui ================= */
vi.mock("src/components/ui", async (importOriginal) => {
  const actualUnknown = await importOriginal();
  const actual = actualUnknown as Record<string, unknown>;

  // Menu always in DOM
  const Root = ({ children }: { readonly children: React.ReactNode }) => (
    <div data-testid="menu-root">{children}</div>
  );
  const Trigger = ({
    asChild,
    children,
  }: { readonly asChild?: boolean; readonly children: React.ReactNode }) =>
    asChild ? (children as React.ReactElement) : <button type="button">{children}</button>;
  const Content = ({ children }: { readonly children: React.ReactNode }) => <div role="menu">{children}</div>;
  const Item = ({
    children,
    disabled,
    onClick,
    value,
  }: {
    readonly children: React.ReactNode;
    readonly disabled?: boolean;
    readonly onClick?: () => void;
    readonly value?: string;
  }) => (
    <button
      aria-label={typeof value === "string" ? value : undefined}
      disabled={disabled}
      onClick={onClick}
      role="menuitem"
      type="button"
    >
      {children}
    </button>
  );

  // Render Select as native <select>
  type RootProps = {
    children?: React.ReactNode;
    collection?: { items?: Array<{ label?: string; textValue?: string; value: string }> } | undefined;
    "data-testid"?: string;
    multiple?: boolean;
    onChange?: (val: Array<string> | string) => void;
    onValueChange?: (val: Array<string> | string) => void;
    value?: Array<string> | string | undefined;
  };

  const SelectRoot = ({ collection, "data-testid": testId, multiple, onChange, onValueChange }: RootProps) => {
    const items: Array<{ label?: string; textValue?: string; value: string }> = Array.isArray(collection?.items)
      ? collection.items
      : [];

    const handleChange = (evt: React.ChangeEvent<HTMLSelectElement>) => {
      if (multiple) {
        const selectedValues = [...evt.currentTarget.selectedOptions].map((opt) => opt.value);

        onValueChange?.(selectedValues);
        onChange?.(selectedValues);
      } else {
        const val = evt.currentTarget.value;

        onValueChange?.(val);
        onChange?.(val);
      }
    };

    return (
      <div className="css-sl2qcv">
        <p className="css-usgcjs" />
        <select data-testid={testId ?? "mock-select"} multiple={Boolean(multiple)} onChange={handleChange}>
          {items.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label ?? opt.value}
            </option>
          ))}
        </select>
      </div>
    );
  };

  const SelectTriggerShim = ({ children }: { readonly children?: React.ReactNode }) => <span>{children}</span>;
  const SelectContentShim = ({ children }: { readonly children?: React.ReactNode }) => <span>{children}</span>;
  const SelectValueTextShim = ({ children }: { readonly children?: React.ReactNode }) => <span>{children}</span>;

  return {
    ...actual,
    Menu: { Content, Item, Root, Trigger },
    Select: {
      Content: SelectContentShim,
      Item: () => <span data-testid="select-item-noop" />,
      ItemText: SelectTriggerShim,
      Root: SelectRoot,
      // Inline simple shims to avoid hoist/unused issues
      Trigger: SelectTriggerShim,
      ValueText: SelectValueTextShim,
    },
  };
});

/* ================= Test data & helpers ================= */

const tasksData = {
  tasks: [
    {
      is_mapped: false,
      operator_name: "BashOperator",
      retries: 1,
      task_display_name: "A step",
      task_id: "a",
      trigger_rule: "all_success",
    },
    {
      is_mapped: true,
      operator_name: "PythonOperator",
      retries: 0,
      task_display_name: "B step",
      task_id: "b",
      trigger_rule: "all_done",
    },
  ] as unknown as Array<TaskResponse>,
  total_entries: 2,
};

const SearchEcho = () => {
  const [sp] = useSearchParams();

  return <div data-testid="router-search">{sp.toString()}</div>;
};

const renderWithRouter = (initial = "/dags/example/tasks"): void => {
  render(
    <MemoryRouter initialEntries={[initial]}>
      <Routes>
        <Route
          element={
            <ChakraWrapper>
              <>
                <TaskFilters tasksData={tasksData} />
                <SearchEcho />
              </>
            </ChakraWrapper>
          }
          path="/dags/:dagId/tasks"
        />
      </Routes>
    </MemoryRouter>,
  );
};

/** Grab the native <select> element inside a label section */
const getSelectInSection = (section: HTMLElement): HTMLSelectElement => {
  const select = section.querySelector("select");

  if (!select) {
    throw new Error("Expected a native <select> in section, found none");
  }

  return select;
};

/** Helper: set values on a native <select> (works for multi and single) */
const setSelectValues = (selectEl: HTMLSelectElement, values: Array<string> | string) => {
  const wanted = Array.isArray(values) ? new Set(values) : new Set([values]);

  for (const opt of selectEl.options) {
    opt.selected = wanted.has(opt.value);
  }
};

describe("TaskFilters", () => {
  it("builds operator options from payload", () => {
    renderWithRouter();

    // Add the "Operators" filter from the + Filter menu (scope to menu root)
    const addFilterBtn1 = within(screen.getByTestId("menu-root")).getByRole("button", {
      name: /(?:common:)?filter/iu,
    });

    fireEvent.click(addFilterBtn1);
    fireEvent.click(screen.getByRole("menuitem", { name: /operator/iu }));

    // Assert native <select> options exist
    const operatorsLabel = screen.getByText(/^Operators\s*:/u);
    const operatorsSection = operatorsLabel.closest("div") as HTMLElement;
    const select = getSelectInSection(operatorsSection);

    within(select).getByRole("option", { name: /bashoperator/iu });
    within(select).getByRole("option", { name: /pythonoperator/iu });
  });

  it("writes selected filters into URL params (text + operator)", async () => {
    renderWithRouter();

    // Add text filter
    const addFilterBtn2 = within(screen.getByTestId("menu-root")).getByRole("button", {
      name: /(?:common:)?filter/iu,
    });

    fireEvent.click(addFilterBtn2);
    fireEvent.click(screen.getByRole("menuitem", { name: /name_pattern/iu }));

    const textbox = screen.getByPlaceholderText(/name contains…/iu);

    fireEvent.change(textbox, { target: { value: "cli_" } });
    fireEvent.keyDown(textbox, { code: "Enter", key: "Enter" });

    // Add Operators filter and pick BashOperator
    const addFilterBtn3 = within(screen.getByTestId("menu-root")).getByRole("button", {
      name: /(?:common:)?filter/iu,
    });

    fireEvent.click(addFilterBtn3);
    fireEvent.click(screen.getByRole("menuitem", { name: /operator/iu }));

    const operatorsLabel = screen.getByText(/^Operators\s*:/u);
    const operatorsSection = operatorsLabel.closest("div") as HTMLElement;
    const select = getSelectInSection(operatorsSection);

    setSelectValues(select, ["BashOperator"]);
    fireEvent.change(select);

    await waitFor(() => {
      const qs = screen.getByTestId("router-search").textContent ?? "";

      expect(qs).toMatch(/name_pattern=cli_/u);
      expect(qs).toMatch(/operator=BashOperator/u);
    });
  });

  it("single-select mapped writes only one value", async () => {
    renderWithRouter();

    // Add the "Mapped" filter
    const addFilterBtn4 = within(screen.getByTestId("menu-root")).getByRole("button", {
      name: /(?:common:)?filter/iu,
    });

    fireEvent.click(addFilterBtn4);
    fireEvent.click(screen.getByRole("menuitem", { name: /^mapped$/iu }));

    // Choose exactly "Mapped" (true)
    const mappedLabel = screen.getByText(/^Mapped\s*:/u);
    const mappedSection = mappedLabel.closest("div") as HTMLElement;
    const select = getSelectInSection(mappedSection);

    setSelectValues(select, "true");
    fireEvent.change(select);

    await waitFor(() => {
      const qs = screen.getByTestId("router-search").textContent ?? "";

      expect(qs).toMatch(/mapped=true/u);
      expect(qs).not.toMatch(/mapped=false/u);
    });
  });
});
