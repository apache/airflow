// src/components/FilterBar/filters/_filters_/SelectFilter.spec.tsx
import { render, screen, fireEvent } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import type { JSX, ReactNode } from "react";

import { SelectFilter as SelectFilterComponent } from "src/components/FilterBar/filters/SelectFilter";

/** Alias to bypass strict prop typing in tests (lets us pass `items`, etc.) */
const SelectFilter = SelectFilterComponent as unknown as (props: Record<string, unknown>) => JSX.Element;

/* ================= Mock src/components/ui ================= */
vi.mock("src/components/ui", async (importOriginal) => {
  const actualUnknown = await importOriginal();
  const actual = actualUnknown as Record<string, unknown>;

  // Minimal Menu mock (always present in DOM)
  const MenuRoot = ({ children }: { readonly children: ReactNode }) => (
    <div data-testid="menu-root">{children}</div>
  );
  const MenuTrigger = ({ children }: { readonly children: ReactNode }) => (
    <button type="button">{children}</button>
  );
  const MenuContent = ({ children }: { readonly children: ReactNode }) => <div role="menu">{children}</div>;
  const MenuItem = ({
    children,
    disabled,
    onClick,
    value,
  }: {
    readonly children: ReactNode;
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

  // Native <select> drop-in for Select.Root
  type SelectRootProps = {
    collection?: { items?: Array<{ label?: string; textValue?: string; value: string }> } | undefined;
    "data-testid"?: string;
    multiple?: boolean;
    onChange?: (val: Array<string> | string) => void;
    onValueChange?: (val: Array<string> | string) => void;
  };

  const SelectRoot = ({
    collection,
    "data-testid": testId,
    multiple,
    onChange,
    onValueChange,
  }: SelectRootProps) => {
    const items = Array.isArray(collection?.items) ? collection.items : [];

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
      <select data-testid={testId ?? "mock-select"} multiple={Boolean(multiple)} onChange={handleChange}>
        {items.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label ?? opt.value}
          </option>
        ))}
      </select>
    );
  };

  return {
    ...actual,
    Menu: { Content: MenuContent, Item: MenuItem, Root: MenuRoot, Trigger: MenuTrigger },
    Select: {
      Content: ({ children }: { readonly children?: ReactNode }) => <span>{children}</span>,
      Item: () => <span data-testid="select-item-noop" />,
      ItemText: ({ children }: { readonly children?: ReactNode }) => <span>{children}</span>,
      Root: SelectRoot,
      // Simple shims to match API surface
      Trigger: ({ children }: { readonly children?: ReactNode }) => <span>{children}</span>,
      ValueText: ({ children }: { readonly children?: ReactNode }) => <span>{children}</span>,
    },
  };
});

/* ================= Helpers ================= */

const getSelectIn = (section: HTMLElement): HTMLSelectElement => {
  const el = section.querySelector("select");

  if (!el) {throw new Error("Expected a native <select> in section, found none");}

  return el;
};

const setSelectValues = (selectEl: HTMLSelectElement, values: Array<string> | string) => {
  const wanted = Array.isArray(values) ? new Set(values) : new Set([values]);

  for (const opt of selectEl.options) {opt.selected = wanted.has(opt.value);}
};

/* ================= Tests ================= */

describe("SelectFilter", () => {
  it("multi-select: toggles values and calls onChange with array", () => {
    const onChange = vi.fn();

    render(
      <SelectFilter
        items={[
          { label: "BashOperator", value: "BashOperator" },
          { label: "PythonOperator", value: "PythonOperator" },
        ]}
        label="Operators"
        multiple
        onChange={onChange}
        value={[]}
      />,
    );

    const labelNode = screen.getByText(/^Operators\s*:/u);
    const section = labelNode.closest("div") as HTMLElement;
    const select = getSelectIn(section);

    setSelectValues(select, ["BashOperator"]);
    fireEvent.change(select);
    expect(onChange).toHaveBeenLastCalledWith(["BashOperator"]);

    setSelectValues(select, ["BashOperator", "PythonOperator"]);
    fireEvent.change(select);
    expect(onChange).toHaveBeenLastCalledWith(["BashOperator", "PythonOperator"]);
  });

  it("single-select: selects one value and calls onChange with string", () => {
    const onChange = vi.fn();

    render(
      <SelectFilter
        items={[
          { label: "Mapped", value: "true" },
          { label: "Unmapped", value: "false" },
        ]}
        label="Mapped"
        multiple={false}
        onChange={onChange}
        value={undefined}
      />,
    );

    const labelNode = screen.getByText(/^Mapped\s*:/u);
    const section = labelNode.closest("div") as HTMLElement;
    const select = getSelectIn(section);

    fireEvent.change(select, { target: { value: "true" } });
    expect(onChange).toHaveBeenLastCalledWith("true");
  });

  it("clear button empties selection (simulate empty multi-select)", () => {
    const onChange = vi.fn();

    render(
      <SelectFilter
        items={[
          { label: "BashOperator", value: "BashOperator" },
          { label: "PythonOperator", value: "PythonOperator" },
        ]}
        label="Operators"
        multiple
        onChange={onChange}
        value={["BashOperator", "PythonOperator"]}
      />,
    );

    const labelNode = screen.getByText(/^Operators\s*:/u);
    const section = labelNode.closest("div") as HTMLElement;
    const select = getSelectIn(section);

    setSelectValues(select, []);
    fireEvent.change(select);
    expect(onChange).toHaveBeenLastCalledWith([]);
  });
});
