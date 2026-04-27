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

import type { ChangeEvent } from "react";
import { createElement } from "react";

import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ParamSchema, ParamsSpec } from "src/queries/useDagParams";
import { Wrapper } from "src/utils/Wrapper";

import TriggerDAGForm from "./TriggerDAGForm";

// Replace the Monaco with a plain textarea so we can assert
// on its value and simulate edit/blur.
vi.mock("src/components/JsonEditor", () => ({
  JsonEditor: ({ onBlur, onChange, value }: { readonly onBlur?: () => void; readonly onChange?: (val: string) => void; readonly value?: string }) =>
    createElement("textarea", {
      "data-testid": "json-editor",
      onBlur: () => onBlur?.(),
      onChange: (e: ChangeEvent<HTMLTextAreaElement>) => onChange?.(e.target.value),
      value: value ?? "",
    }),
}));

const dagParams: ParamsSpec = {
  "last-x-days": {
    description: "Number of days to import",
    schema: { title: "last-x-days", type: "integer" } as ParamSchema,
    value: 7,
  },
  "target-table": {
    description: "Target snowflake table",
    schema: { title: "target-table", type: "string" } as ParamSchema,
    value: "default-table",
  },
};

vi.mock("src/queries/useDagParams", () => ({
  useDagParams: () => ({ paramsDict: dagParams }),
}));

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: () => ({ mutate: vi.fn() }),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

describe("TriggerDAGForm", () => {
  it("submits edited param values when retriggering with prefilled config", async () => {
    const onSubmit = vi.fn();

    const prefillConfig = {
      conf: { "last-x-days": 3, "target-table": "prefilled-table" } as Record<string, unknown>,
      logicalDate: undefined,
      runId: "previous-run-id",
    };

    render(
      <TriggerDAGForm
        dagDisplayName="hello"
        dagId="hello"
        hasSchedule={false}
        isPartitioned={false}
        isPaused={false}
        onSubmitTrigger={onSubmit}
        open
        prefillConfig={prefillConfig}
      />,
      { wrapper: Wrapper },
    );

    const tableInput = document.querySelector<HTMLInputElement>('input[name="element_target-table"]');

    expect(tableInput).toHaveValue("prefilled-table");

    // Edit the field.
    fireEvent.change(tableInput!, { target: { value: "new-table" } });

    const triggerButton = screen.getByTestId("trigger-dag-submit");

    triggerButton.click();

    await waitFor(() => {
      expect(onSubmit).toHaveBeenCalledOnce();
    });

    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        conf: JSON.stringify({ "last-x-days": 3, "target-table": "new-table" }, undefined, 2),
      }),
    );
  });

  it("reflects form field edits in the JSON editor", async () => {
    render(
      <TriggerDAGForm
        dagDisplayName="hello"
        dagId="hello"
        hasSchedule={false}
        isPartitioned={false}
        isPaused={false}
        open
      />,
      { wrapper: Wrapper },
    );

    const [tableInput, daysInput] = await waitFor(() => {
      const table = document.querySelector<HTMLInputElement>('input[name="element_target-table"]');
      const days = document.querySelector<HTMLInputElement>('input[name="element_last-x-days"]');

      expect(table).toBeInTheDocument();
      expect(days).toBeInTheDocument();

      return [table!, days!] as const;
    });

    expect(tableInput).toHaveValue("default-table");
    expect(daysInput).toHaveValue("7");

    fireEvent.change(tableInput, { target: { value: "updated-via-form" } });

    const jsonEditor = screen.getByTestId("json-editor");

    await waitFor(() => {
      const parsed = JSON.parse((jsonEditor as HTMLTextAreaElement).value) as Record<string, unknown>;

      expect(parsed).toEqual({ "last-x-days": 7, "target-table": "updated-via-form" });
    });
  });

  it("reflects JSON editor edits in form fields", async () => {
    render(
      <TriggerDAGForm
        dagDisplayName="hello"
        dagId="hello"
        hasSchedule={false}
        isPartitioned={false}
        isPaused={false}
        open
      />,
      { wrapper: Wrapper },
    );

    const [tableInput, daysInput] = await waitFor(() => {
      const table = document.querySelector<HTMLInputElement>('input[name="element_target-table"]');
      const days = document.querySelector<HTMLInputElement>('input[name="element_last-x-days"]');

      expect(table).toBeInTheDocument();
      expect(days).toBeInTheDocument();

      return [table!, days!] as const;
    });

    expect(tableInput).toHaveValue("default-table");
    expect(daysInput).toHaveValue("7");

    const jsonEditor = screen.getByTestId("json-editor");
    const newConf = JSON.stringify({ "last-x-days": 42, "target-table": "updated-via-json" }, undefined, 2);

    // Blur to trigger store update.
    fireEvent.change(jsonEditor, { target: { value: newConf } });
    fireEvent.blur(jsonEditor);

    await waitFor(() => {
      expect(document.querySelector('input[name="element_target-table"]')).toHaveValue("updated-via-json");
    });

    expect(document.querySelector('input[name="element_last-x-days"]')).toHaveValue("42");
  });

  it("submits edited param values from a normal trigger", async () => {
    const onSubmit = vi.fn();

    render(
      <TriggerDAGForm
        dagDisplayName="hello"
        dagId="hello"
        hasSchedule={false}
        isPartitioned={false}
        isPaused={false}
        onSubmitTrigger={onSubmit}
        open
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() =>
      expect(document.querySelector('input[name="element_target-table"]')).toBeInTheDocument(),
    );

    const input = document.querySelector<HTMLInputElement>('input[name="element_target-table"]');

    expect(input).toHaveValue("default-table");

    fireEvent.change(input!, { target: { value: "edited-table" } });

    const triggerButton = screen.getByTestId("trigger-dag-submit");

    triggerButton.click();

    await waitFor(() => {
      expect(onSubmit).toHaveBeenCalledOnce();
    });

    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        conf: JSON.stringify({ "last-x-days": 7, "target-table": "edited-table" }, undefined, 2),
      }),
    );
  });
});
