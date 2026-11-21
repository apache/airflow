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
import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { Wrapper } from "src/utils/Wrapper";

import {
  buildForm,
  dagParams,
  getPartitionKeyInput,
  PLAIN_HELP,
  prefillConfig,
  RECENT_CONF,
  selectRecentConfig,
  SUGGESTED_HELP,
} from "./TriggerDAGForm.testUtils";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (translationKey: string) => translationKey,
  }),
}));

const useDagParamsMock = vi.hoisted(() => vi.fn());

vi.mock("src/queries/useDagParams", () => ({
  useDagParams: useDagParamsMock,
}));

const togglePauseMock = vi.hoisted(() => vi.fn());

vi.mock("src/queries/useTogglePause", () => ({
  useTogglePause: () => ({
    mutate: togglePauseMock,
  }),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDagRunServiceGetDagRuns: vi.fn(() => ({
      data: {
        dag_runs: [
          {
            conf: { message: "From recent" },
            dag_run_id: "run_recent",
            run_after: "2025-01-01T00:00:00Z",
          },
        ],
      },
      isFetchedAfterMount: true,
      isFetching: false,
      isSuccess: true,
    })),
  };
});

vi.mock("../DateTimeInput", () => ({
  DateTimeInput: ({ value = "" }: { readonly value?: string }) => (
    <input aria-label="Logical Date" readOnly value={value} />
  ),
}));

vi.mock("../JsonEditor", () => ({
  JsonEditor: ({
    onBlur,
    onChange,
    value = "",
  }: {
    readonly onBlur?: () => void;
    readonly onChange?: (value: string) => void;
    readonly value?: string;
  }) => (
    <textarea
      aria-label="Configuration JSON"
      onBlur={onBlur}
      onChange={(event) => onChange?.(event.target.value)}
      value={value}
    />
  ),
}));

describe("TriggerDAGForm partition key suggestion", () => {
  beforeEach(() => {
    useDagParamsMock.mockReturnValue(dagParams);
  });

  it("pre-fills the partition key field with the suggested value", async () => {
    const { container } = render(buildForm({ suggestedPartitionKey: "2026-02-18T00:00:00" }), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="partitionKey"]')).toBeInTheDocument(),
    );
    const partitionKeyField = container.querySelector<HTMLInputElement>('input[name="partitionKey"]');

    expect(partitionKeyField?.value).toBe("2026-02-18T00:00:00");
    expect(screen.getByText(SUGGESTED_HELP)).toBeInTheDocument();
    expect(screen.queryByText(PLAIN_HELP)).not.toBeInTheDocument();
  });

  it("leaves the partition key field blank when no suggestion is provided", async () => {
    const { container } = render(buildForm({}), { wrapper: Wrapper });

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="partitionKey"]')).toBeInTheDocument(),
    );
    const partitionKeyField = container.querySelector<HTMLInputElement>('input[name="partitionKey"]');

    expect(partitionKeyField?.value).toBe("");
    expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument();
    expect(screen.queryByText(SUGGESTED_HELP)).not.toBeInTheDocument();
  });

  it("applies a fresher suggestion arriving after the form is shown", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "2026-02-18T00:00:00" }), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="partitionKey"]')).toBeInTheDocument(),
    );

    rerender(buildForm({ suggestedPartitionKey: "2026-02-18T01:00:00" }));

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="partitionKey"]')?.value).toBe(
        "2026-02-18T01:00:00",
      ),
    );
    expect(screen.getByText(SUGGESTED_HELP)).toBeInTheDocument();
  });

  it("keeps an edited partition key when a fresher suggestion arrives", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "2026-02-18T00:00:00" }), {
      wrapper: Wrapper,
    });

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="partitionKey"]')).toBeInTheDocument(),
    );

    const partitionKeyField = container.querySelector<HTMLInputElement>('input[name="partitionKey"]');

    fireEvent.change(partitionKeyField as HTMLInputElement, { target: { value: "2026-01-01T00:00:00" } });

    rerender(buildForm({ suggestedPartitionKey: "2026-02-18T01:00:00" }));

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="partitionKey"]')?.value).toBe(
        "2026-01-01T00:00:00",
      ),
    );
    expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument();
    expect(screen.queryByText(SUGGESTED_HELP)).not.toBeInTheDocument();
  });

  it("applies a suggestion that arrives after the form first rendered", async () => {
    const { container, rerender } = render(buildForm({}), { wrapper: Wrapper });

    expect(getPartitionKeyInput(container)).toHaveValue("");

    rerender(buildForm({ suggestedPartitionKey: "2026-02-18T00:00:00" }));

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("2026-02-18T00:00:00"));
    expect(screen.getByText(SUGGESTED_HELP)).toBeInTheDocument();
  });

  it("does not refill a partition key the user cleared when a new suggestion arrives", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "" } });
    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue(""));

    rerender(buildForm({ suggestedPartitionKey: "b" }));

    // Let any effect triggered by the new suggestion run before asserting that nothing was refilled.
    await waitFor(() => expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument());
    expect(getPartitionKeyInput(container)).toHaveValue("");
  });

  it("clears the partition key when the suggestion goes away and the user has not edited it", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("a"));

    rerender(buildForm({ suggestedPartitionKey: null }));

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue(""));
    expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument();
    expect(screen.queryByText(SUGGESTED_HELP)).not.toBeInTheDocument();
  });

  it("keeps the partition key the user typed when the suggestion goes away", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "mine" } });
    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("mine"));

    rerender(buildForm({ suggestedPartitionKey: null }));

    await waitFor(() => expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument());
    expect(getPartitionKeyInput(container)).toHaveValue("mine");
  });

  it.each([
    {
      changeConf: async () => {
        await selectRecentConfig();
        await waitFor(() => expect(screen.getByLabelText("Configuration JSON")).toHaveValue(RECENT_CONF));
      },
      scenario: "a recent configuration is selected",
    },
    {
      changeConf: async () => {
        const configJson = screen.getByLabelText("Configuration JSON");

        fireEvent.change(configJson, { target: { value: '{"message": "edited"}' } });
        fireEvent.blur(configJson);
        await waitFor(() =>
          expect(screen.getByLabelText<HTMLTextAreaElement>("Configuration JSON").value).toContain("edited"),
        );
      },
      scenario: "the conf is edited and the editor loses focus",
    },
  ])("keeps the user's partition key when $scenario and a new suggestion arrives", async ({ changeConf }) => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "mine" } });
    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("mine"));

    await changeConf();
    rerender(buildForm({ suggestedPartitionKey: "b" }));

    await waitFor(() => expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument());
    expect(getPartitionKeyInput(container)).toHaveValue("mine");
  });

  it("keeps following the suggestion after the conf changes when the user has not edited the field", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("a"));
    await selectRecentConfig();
    await waitFor(() => expect(screen.getByLabelText("Configuration JSON")).toHaveValue(RECENT_CONF));

    rerender(buildForm({ suggestedPartitionKey: "b" }));

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("b"));
  });

  it("keeps the note, run id and conf of a re-triggered run when the suggestion changes", async () => {
    const { container, rerender } = render(buildForm({ prefillConfig, suggestedPartitionKey: "a" }), {
      wrapper: Wrapper,
    });

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("a"));
    await waitFor(() =>
      expect(screen.getByLabelText<HTMLTextAreaElement>("Configuration JSON").value).toContain('"value"'),
    );
    fireEvent.change(screen.getByLabelText("runId"), { target: { value: "my_run" } });
    fireEvent.click(screen.getByText("note.placeholder"));
    await waitFor(() =>
      expect(container.querySelector('textarea[data-scope="editable"]')).toBeInTheDocument(),
    );
    fireEvent.change(container.querySelector('textarea[data-scope="editable"]') as HTMLTextAreaElement, {
      target: { value: "my note" },
    });
    await waitFor(() => expect(screen.getByDisplayValue("my note")).toBeInTheDocument());

    rerender(buildForm({ prefillConfig, suggestedPartitionKey: "b" }));

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("b"));
    expect(screen.getByLabelText("runId")).toHaveValue("my_run");
    expect(screen.getByDisplayValue("my note")).toBeInTheDocument();
    expect(screen.getByLabelText<HTMLTextAreaElement>("Configuration JSON").value).toContain('"value"');
  });

  it("follows the current suggestion again after the form is closed and reopened", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "mine" } });
    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("mine"));

    rerender(buildForm({ open: false, suggestedPartitionKey: "a" }));
    rerender(buildForm({ suggestedPartitionKey: "b" }));

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("b"));
    expect(screen.getByText(SUGGESTED_HELP)).toBeInTheDocument();
  });

  it("treats a value the user retyped as the suggestion as their own edit", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "mine" } });
    fireEvent.change(getPartitionKeyInput(container), { target: { value: "a" } });
    await waitFor(() => expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument());

    rerender(buildForm({ suggestedPartitionKey: "b" }));

    await waitFor(() => expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument());
    expect(getPartitionKeyInput(container)).toHaveValue("a");
    expect(screen.queryByText(SUGGESTED_HELP)).not.toBeInTheDocument();
  });

  it("does not call a user-typed value the suggestion once it matches an outdated suggestion", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "mine" } });
    rerender(buildForm({ suggestedPartitionKey: "b" }));
    fireEvent.change(getPartitionKeyInput(container), { target: { value: "a" } });

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("a"));
    expect(screen.getByText(PLAIN_HELP)).toBeInTheDocument();
    expect(screen.queryByText(SUGGESTED_HELP)).not.toBeInTheDocument();
  });

  it("shows the suggestion again after the form is closed and reopened with an unchanged suggestion", async () => {
    const { container, rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("a"));

    rerender(buildForm({ open: false, suggestedPartitionKey: "a" }));
    rerender(buildForm({ suggestedPartitionKey: "a" }));

    await waitFor(() => expect(screen.getByText(SUGGESTED_HELP)).toBeInTheDocument());
    expect(getPartitionKeyInput(container)).toHaveValue("a");
  });

  it("follows the suggestion again after the prefilled run is swapped for another while open", async () => {
    const { container, rerender } = render(buildForm({ prefillConfig, suggestedPartitionKey: "a" }), {
      wrapper: Wrapper,
    });

    const swappedPrefill = { ...prefillConfig, conf: { other: "conf" }, runId: "another_run" };

    fireEvent.change(getPartitionKeyInput(container), { target: { value: "mine" } });
    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("mine"));

    rerender(buildForm({ prefillConfig: swappedPrefill, suggestedPartitionKey: "a" }));
    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("a"));

    rerender(buildForm({ prefillConfig: swappedPrefill, suggestedPartitionKey: "b" }));

    await waitFor(() => expect(getPartitionKeyInput(container)).toHaveValue("b"));
    expect(screen.getByText(SUGGESTED_HELP)).toBeInTheDocument();
  });

  it("does not apply a suggestion to a Dag that is not partitioned", async () => {
    const onSubmitTrigger = vi.fn();
    const { rerender } = render(buildForm({ isPartitioned: false, onSubmitTrigger }), { wrapper: Wrapper });

    rerender(
      buildForm({ isPartitioned: false, onSubmitTrigger, suggestedPartitionKey: "2026-02-18T00:00:00" }),
    );
    fireEvent.click(screen.getByTestId("trigger-dag-submit"));

    await waitFor(() => expect(onSubmitTrigger).toHaveBeenCalledOnce());
    expect(onSubmitTrigger).toHaveBeenCalledWith(
      expect.objectContaining({ partitionKey: undefined }) as unknown,
    );
  });
});
