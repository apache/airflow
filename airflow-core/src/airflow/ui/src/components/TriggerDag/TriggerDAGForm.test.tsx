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
/* eslint-disable max-lines */
import type { ComponentProps } from "react";

import "@testing-library/jest-dom/vitest";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { MOCK_DAG } from "src/mocks/handlers/dag";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import TriggerDAGForm from "./TriggerDAGForm";

const dagParams = vi.hoisted(() => ({
  paramsDict: {
    message: {
      description: "Message",
      schema: {
        title: "Message",
        type: "string",
      },
      value: "Hello",
    },
  },
}));

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

const prefillConfig = { conf: { key: "value" }, logicalDate: undefined, runId: "prior_run" };

const buildForm = (props: Partial<ComponentProps<typeof TriggerDAGForm>>) => (
  <TriggerDAGForm
    dagId="example_partitioned_dag"
    error={undefined}
    hasSchedule={false}
    isPartitioned
    isPartitionedAtRuntime={false}
    isPaused={false}
    isPending={false}
    onSubmitTrigger={vi.fn()}
    open
    {...props}
  />
);

const RECENT_CONF = JSON.stringify({ message: "From recent" }, undefined, 2);

const selectRecentConfig = async () => {
  fireEvent.click(within(screen.getByTestId("recent-config-select")).getByRole("combobox"));
  await waitFor(() => expect(screen.getByRole("listbox")).toBeInTheDocument());
  fireEvent.click(screen.getByText("run_recent"));
};

describe("TriggerDAGForm", () => {
  beforeEach(() => {
    useDagParamsMock.mockReturnValue(dagParams);
    togglePauseMock.mockClear();
  });

  it.each([
    { action: undefined, drainDag: false, unpauses: true },
    { action: "pausedDag.drain", drainDag: true, unpauses: false },
    { action: "pausedDag.keepPaused", drainDag: false, unpauses: false },
  ])(
    "submits a paused Dag with drainDag=$drainDag when choosing $action",
    async ({ action, drainDag, unpauses }) => {
      const onSubmitTrigger = vi.fn();

      render(
        <TriggerDAGForm
          dagId="paused_dag"
          error={undefined}
          hasSchedule={false}
          isPartitioned={false}
          isPartitionedAtRuntime={false}
          isPaused
          isPending={false}
          onSubmitTrigger={onSubmitTrigger}
          open
        />,
        { wrapper: Wrapper },
      );

      if (action !== undefined) {
        fireEvent.click(screen.getByText(action));
        await waitFor(() => expect(screen.getByRole("radio", { name: action })).toBeChecked());
      }
      fireEvent.click(screen.getByTestId("trigger-dag-submit"));

      await waitFor(() =>
        expect(onSubmitTrigger).toHaveBeenCalledWith(expect.objectContaining({ drainDag })),
      );
      if (unpauses) {
        expect(togglePauseMock).toHaveBeenCalledWith({
          dagId: "paused_dag",
          requestBody: { is_paused: false },
        });
      } else {
        expect(togglePauseMock).not.toHaveBeenCalled();
      }
    },
  );

  it("does not offer the paused Dag choices for an active Dag", () => {
    render(
      <TriggerDAGForm
        dagId="active_dag"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
      />,
      { wrapper: Wrapper },
    );

    expect(screen.queryByText("pausedDag.drain")).not.toBeInTheDocument();
  });

  it.each([
    {
      dates: { end_date: null, start_date: "2000-01-01T00:00:00Z" },
      scenario: "within the Dag's dates",
      text: "components:triggerDag.dagStartEndDate.start",
    },
    {
      dates: { end_date: null, start_date: "2999-01-01T00:00:00Z" },
      scenario: "before the Dag's start date",
      text: "components:triggerDag.dagStartEndDate.noTaskWillRun components:triggerDag.dagStartEndDate.start",
    },
    {
      dates: { end_date: "2000-01-01T00:00:00Z", start_date: "1999-01-01T00:00:00Z" },
      scenario: "after the Dag's end date",
      text: "components:triggerDag.dagStartEndDate.noTaskWillRun components:triggerDag.dagStartEndDate.both",
    },
  ])(
    "shows the Dag start and end dates when the default logical date is $scenario",
    async ({ dates, text }) => {
      server.use(
        http.get("/api/v2/dags/dated_dag/details", () =>
          HttpResponse.json({ ...MOCK_DAG, ...dates, dag_id: "dated_dag" }),
        ),
      );

      render(
        <TriggerDAGForm
          dagId="dated_dag"
          error={undefined}
          hasSchedule={false}
          isPartitioned={false}
          isPartitionedAtRuntime={false}
          isPaused={false}
          isPending={false}
          onSubmitTrigger={vi.fn()}
          open
        />,
        { wrapper: Wrapper },
      );

      expect(await screen.findByText(text)).toBeInTheDocument();
    },
  );

  it("propagates the selected run's conf to the JSON editor when the Dag has no declared params", async () => {
    useDagParamsMock.mockReturnValue({ paramsDict: {} });

    render(
      <TriggerDAGForm
        dagId="example_no_params"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
        prefillConfig={{
          conf: { message: "from selected run" },
          logicalDate: undefined,
          runId: "manual__test",
        }}
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() => {
      const configJson = screen.getByLabelText("Configuration JSON");

      if (!(configJson instanceof HTMLTextAreaElement)) {
        throw new TypeError("Expected Configuration JSON to render as a textarea");
      }

      expect(configJson.value).toContain('"from selected run"');
    });
  });

  it("syncs the Configuration JSON after Run Parameters edits in prefilled re-trigger mode", async () => {
    const { container } = render(
      <TriggerDAGForm
        dagId="example_params_trigger_ui"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
        prefillConfig={{
          conf: {
            message: "Original message",
          },
          logicalDate: undefined,
          runId: "manual__test",
        }}
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="element_message"]')).toBeInTheDocument(),
    );
    const messageField = container.querySelector<HTMLInputElement>('input[name="element_message"]');

    if (messageField === null) {
      throw new Error("Expected message input to be rendered");
    }

    fireEvent.change(messageField, { target: { value: "Updated message" } });

    await waitFor(() => {
      const configJson = screen.getByLabelText("Configuration JSON");

      if (!(configJson instanceof HTMLTextAreaElement)) {
        throw new TypeError("Expected Configuration JSON to render as a textarea");
      }

      expect(configJson.value).toContain('"Updated message"');
    });
  });

  it("hides the partition key field for non-partitioned Dags", async () => {
    render(
      <TriggerDAGForm
        dagId="example_non_partitioned_dag"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(screen.getByText("runId")).toBeInTheDocument());
    expect(screen.queryByText("dagRun.partitionKey")).not.toBeInTheDocument();
  });

  it("shows the partition key field for partitioned Dags", async () => {
    render(
      <TriggerDAGForm
        dagId="example_partitioned_dag"
        error={undefined}
        hasSchedule={false}
        isPartitioned
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(screen.getByText("dagRun.partitionKey")).toBeInTheDocument());
    expect(screen.getByText("components:triggerDag.partitionKeyHelp")).toBeInTheDocument();
  });

  it("prefills the form when a recent configuration is selected from the dropdown", async () => {
    const { container } = render(
      <TriggerDAGForm
        dagId="example_params_trigger_ui"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
        prefillConfig={undefined}
      />,
      { wrapper: Wrapper },
    );

    const recentConfigSelect = screen.getByTestId("recent-config-select");

    fireEvent.click(within(recentConfigSelect).getByRole("combobox"));
    await waitFor(() => expect(screen.getByRole("listbox")).toBeInTheDocument());
    fireEvent.click(screen.getByText("run_recent"));

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="element_message"]')?.value).toBe(
        "From recent",
      ),
    );

    await waitFor(() => {
      const configJson = screen.getByLabelText("Configuration JSON");

      if (!(configJson instanceof HTMLTextAreaElement)) {
        throw new TypeError("Expected Configuration JSON to render as a textarea");
      }

      expect(configJson.value).toContain('"From recent"');
    });
  });

  it("keeps user-entered run id and note when a recent configuration is selected", async () => {
    const { container } = render(
      <TriggerDAGForm
        dagId="example_params_trigger_ui"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
        prefillConfig={undefined}
      />,
      { wrapper: Wrapper },
    );

    const runIdInput = await screen.findByLabelText("runId");

    fireEvent.change(runIdInput, { target: { value: "my_custom_run" } });

    const recentConfigSelect = screen.getByTestId("recent-config-select");

    fireEvent.click(within(recentConfigSelect).getByRole("combobox"));
    await waitFor(() => expect(screen.getByRole("listbox")).toBeInTheDocument());
    fireEvent.click(screen.getByText("run_recent"));

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="element_message"]')?.value).toBe(
        "From recent",
      ),
    );
    expect(screen.getByLabelText("runId")).toHaveValue("my_custom_run");
  });

  it("hides the recent configuration dropdown when re-triggering with a prior run's config", async () => {
    const { container } = render(
      <TriggerDAGForm
        dagId="example_params_trigger_ui"
        error={undefined}
        hasSchedule={false}
        isPartitioned={false}
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
        prefillConfig={{
          conf: { message: "Original message" },
          logicalDate: undefined,
          runId: "manual__test",
        }}
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() =>
      expect(container.querySelector<HTMLInputElement>('input[name="element_message"]')?.value).toBe(
        "Original message",
      ),
    );
    expect(screen.queryByTestId("recent-config-select")).not.toBeInTheDocument();
  });

  it("shows the partition key field for partitioned_at_runtime Dags even though isPartitioned is false", async () => {
    render(
      <TriggerDAGForm
        dagId="example_partitioned_at_runtime_dag"
        error={undefined}
        hasSchedule
        isPartitioned={false}
        isPartitionedAtRuntime
        isPaused={false}
        isPending={false}
        onSubmitTrigger={vi.fn()}
        open
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(screen.getByText("dagRun.partitionKey")).toBeInTheDocument());
    expect(screen.getByText("components:triggerDag.partitionKeyHelp")).toBeInTheDocument();
  });

  it("pre-fills the suggested partition key for an asset-driven Dag and warns the value is a suggestion", async () => {
    const onSubmitTrigger = vi.fn();

    render(
      <TriggerDAGForm
        dagId="example_partitioned_dag"
        error={undefined}
        hasSchedule={false}
        isPartitioned
        isPartitionedAtRuntime={false}
        isPaused={false}
        isPending={false}
        onSubmitTrigger={onSubmitTrigger}
        open
        suggestedPartitionKey="2024-01-01"
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(screen.getByDisplayValue("2024-01-01")).toBeInTheDocument());
    expect(screen.getByText("components:triggerDag.partitionKeySuggestedHelp")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("trigger-dag-submit"));

    await waitFor(() =>
      expect(onSubmitTrigger).toHaveBeenCalledWith(expect.objectContaining({ partitionKey: "2024-01-01" })),
    );
  });

  it("pre-fills the suggested partition key for a partitioned_at_runtime Dag and warns the value is a suggestion", async () => {
    const onSubmitTrigger = vi.fn();

    render(
      <TriggerDAGForm
        dagId="example_partitioned_at_runtime_dag"
        error={undefined}
        hasSchedule
        isPartitioned={false}
        isPartitionedAtRuntime
        isPaused={false}
        isPending={false}
        onSubmitTrigger={onSubmitTrigger}
        open
        suggestedPartitionKey="2024-01-01"
      />,
      { wrapper: Wrapper },
    );

    await waitFor(() => expect(screen.getByDisplayValue("2024-01-01")).toBeInTheDocument());
    expect(screen.getByText("components:triggerDag.partitionKeySuggestedHelp")).toBeInTheDocument();

    fireEvent.click(screen.getByTestId("trigger-dag-submit"));

    await waitFor(() =>
      expect(onSubmitTrigger).toHaveBeenCalledWith(expect.objectContaining({ partitionKey: "2024-01-01" })),
    );
  });

  it("applies a suggested partition key that arrives after the form first rendered", async () => {
    // The Dag query resolves after mount, so defaultValues captured no suggestion.
    const { rerender } = render(buildForm({ prefillConfig }), { wrapper: Wrapper });

    rerender(buildForm({ prefillConfig, suggestedPartitionKey: "2024-01-01" }));

    await waitFor(() => expect(screen.getByDisplayValue("2024-01-01")).toBeInTheDocument());
  });

  it("replaces a suggested partition key with an updated suggestion when the user has not edited it", async () => {
    const { rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    await waitFor(() => expect(screen.getByDisplayValue("a")).toBeInTheDocument());

    rerender(buildForm({ suggestedPartitionKey: "b" }));

    await waitFor(() => expect(screen.getByDisplayValue("b")).toBeInTheDocument());
  });

  it("keeps the user's partition key and conf when a new suggestion arrives", async () => {
    const { rerender } = render(buildForm({ prefillConfig, suggestedPartitionKey: "a" }), {
      wrapper: Wrapper,
    });

    fireEvent.change(await screen.findByDisplayValue("a"), { target: { value: "mine" } });
    await waitFor(() => expect(screen.getByDisplayValue("mine")).toBeInTheDocument());

    rerender(buildForm({ prefillConfig, suggestedPartitionKey: "b" }));

    await waitFor(() => expect(screen.getByDisplayValue("mine")).toBeInTheDocument());
    expect(screen.queryByDisplayValue("b")).not.toBeInTheDocument();
    expect(screen.getByLabelText<HTMLTextAreaElement>("Configuration JSON").value).toContain('"value"');
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
  ])("keeps the user's partition key when $scenario", async ({ changeConf }) => {
    render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    fireEvent.change(await screen.findByDisplayValue("a"), { target: { value: "mine" } });
    await waitFor(() => expect(screen.getByDisplayValue("mine")).toBeInTheDocument());

    await changeConf();

    expect(screen.getByDisplayValue("mine")).toBeInTheDocument();
    expect(screen.queryByDisplayValue("a")).not.toBeInTheDocument();
  });

  it("keeps following the suggestion after the conf changes when the user has not edited the field", async () => {
    const { rerender } = render(buildForm({ suggestedPartitionKey: "a" }), { wrapper: Wrapper });

    await waitFor(() => expect(screen.getByDisplayValue("a")).toBeInTheDocument());
    await selectRecentConfig();
    await waitFor(() => expect(screen.getByLabelText("Configuration JSON")).toHaveValue(RECENT_CONF));

    rerender(buildForm({ suggestedPartitionKey: "b" }));

    await waitFor(() => expect(screen.getByDisplayValue("b")).toBeInTheDocument());
  });
});
