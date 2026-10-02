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
import type { ReactNode } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { UseDagServiceGetDagKeyFn } from "openapi/queries";
import { CancelablePromise } from "openapi/requests/core/CancelablePromise";
import { BackfillService, DagRunService, DagService } from "openapi/requests/services.gen";
import type { DAGResponse, TriggerDagRunResponse } from "openapi/requests/types.gen";

import type * as Ui from "src/system-components";

import { Wrapper } from "src/utils/Wrapper";

import TriggerDAGModal from "./TriggerDAGModal";

vi.mock("src/system-components", async (importOriginal) => {
  const actual = await importOriginal<typeof Ui>();

  return {
    ...actual,
    Modal: ({ children, open }: { readonly children?: ReactNode; readonly open?: boolean }) =>
      open ? <div>{children}</div> : undefined,
  };
});

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));
vi.mock("src/queries/useDagParams", () => ({
  useDagParams: () => ({ paramsDict: {} }),
}));
vi.mock("src/queries/useParamStore", () => ({
  useParamStore: () => ({ conf: "{}", initialParamDict: {}, setConf: vi.fn(), setInitialParamDict: vi.fn() }),
}));
vi.mock("../ConfigForm", () => ({ default: () => <div /> }));
vi.mock("../DateTimeInput", () => ({
  DateTimeInput: ({ value }: { readonly value?: string }) => (
    <input aria-label="Logical Date" readOnly value={value} />
  ),
}));
vi.mock("../DagActions/RunBackfillForm", () => ({
  default: ({ disabled }: { readonly disabled?: boolean }) => (
    <button disabled={disabled} type="button">
      submit backfill
    </button>
  ),
}));

const DAG_ID = "paused_dag";
const dag = {
  dag_id: DAG_ID,
  is_backfillable: true,
  is_paused: true,
  timetable_partitioned: false,
  timetable_summary: null,
} as DAGResponse;

const renderModal = (queryClient: QueryClient, onClose = vi.fn()) =>
  render(
    <QueryClientProvider client={queryClient}>
      <TriggerDAGModal dagDisplayName={DAG_ID} dagId={DAG_ID} onClose={onClose} open />
    </QueryClientProvider>,
    { wrapper: Wrapper },
  );

const createQueryClient = () =>
  new QueryClient({
    defaultOptions: { mutations: { retry: false }, queries: { retry: false, staleTime: 5 * 60 * 1000 } },
  });

describe("TriggerDAGModal", () => {
  beforeEach(() => {
    vi.spyOn(DagService, "getDag").mockResolvedValue(dag);
    vi.spyOn(BackfillService, "listBackfillsUi").mockResolvedValue({ backfills: [], total_entries: 0 });
    vi.spyOn(DagRunService, "getDagRuns").mockResolvedValue({ dag_runs: [], total_entries: 0 });
    vi.spyOn(DagRunService, "triggerDagRun").mockResolvedValue({
      dag_id: DAG_ID,
      dag_run_id: "manual__run",
    } as TriggerDagRunResponse);
  });
  afterEach(() => vi.restoreAllMocks());

  it("passes the paused state it fetches on open to the form", async () => {
    renderModal(createQueryClient());
    expect(await screen.findByText("pausedDag.title")).toBeVisible();
    expect(DagService.getDag).toHaveBeenCalledWith({ dagId: DAG_ID });
  });

  it.each([false, true])(
    "blocks submission until the cached paused=%s state is refreshed",
    async (cachedPaused) => {
      let resolveDag: ((value: DAGResponse) => void) | undefined;

      vi.mocked(DagService.getDag).mockReturnValue(
        new CancelablePromise((resolve) => {
          resolveDag = resolve;
        }),
      );
      const queryClient = createQueryClient();

      queryClient.setQueryData(UseDagServiceGetDagKeyFn({ dagId: DAG_ID }), {
        ...dag,
        is_paused: cachedPaused,
      });
      renderModal(queryClient);
      const logicalDate = screen.getByLabelText("Logical Date");
      const submit = screen.getByTestId("trigger-dag-submit");

      if (cachedPaused) {
        fireEvent.click(screen.getByText("pausedDag.drain"));
      }
      expect(submit).toBeDisabled();
      fireEvent.click(submit);
      expect(DagRunService.triggerDagRun).not.toHaveBeenCalled();

      act(() => {
        resolveDag?.({ ...dag, is_paused: !cachedPaused });
      });
      await waitFor(() => expect(submit).toBeEnabled());
      expect(screen.getByLabelText("Logical Date")).toBe(logicalDate);
      if (!cachedPaused) {
        fireEvent.click(screen.getByText("pausedDag.drain"));
        await waitFor(() => expect(screen.getByRole("radio", { name: "pausedDag.drain" })).toBeChecked());
      }
      fireEvent.click(submit);
      await waitFor(() =>
        expect(DagRunService.triggerDagRun).toHaveBeenCalledWith(
          expect.objectContaining({
            requestBody: expect.objectContaining({ drain_dag: !cachedPaused }) as unknown,
          }),
        ),
      );
    },
  );

  it("also blocks backfill submission while refreshing the Dag", async () => {
    let resolveDag: ((value: DAGResponse) => void) | undefined;

    vi.mocked(DagService.getDag).mockReturnValue(
      new CancelablePromise((resolve) => {
        resolveDag = resolve;
      }),
    );
    const queryClient = createQueryClient();

    queryClient.setQueryData(UseDagServiceGetDagKeyFn({ dagId: DAG_ID }), dag);
    renderModal(queryClient);
    fireEvent.click(screen.getByText("backfill.selectLabel"));
    expect(await screen.findByText("submit backfill")).toBeDisabled();
    act(() => {
      resolveDag?.(dag);
    });
    await waitFor(() => expect(screen.getByText("submit backfill")).toBeEnabled());
  });

  it("does not allow a cached Dag to be submitted after its refresh fails", async () => {
    vi.mocked(DagService.getDag).mockRejectedValue(new Error("Unavailable"));
    const queryClient = createQueryClient();

    queryClient.setQueryData(UseDagServiceGetDagKeyFn({ dagId: DAG_ID }), dag);
    renderModal(queryClient);
    expect(await screen.findByText("triggerDag.loadingFailed")).toBeVisible();
    expect(screen.queryByTestId("trigger-dag-submit")).not.toBeInTheDocument();
    expect(DagRunService.triggerDagRun).not.toHaveBeenCalled();
  });

  it("keeps a pending request disabled when the paused Dag choice changes", async () => {
    let resolveRun: ((value: TriggerDagRunResponse) => void) | undefined;

    vi.mocked(DagRunService.triggerDagRun).mockReturnValue(
      new CancelablePromise((resolve) => {
        resolveRun = resolve;
      }),
    );
    const onClose = vi.fn();

    renderModal(createQueryClient(), onClose);
    fireEvent.click(await screen.findByText("pausedDag.drain"));
    await waitFor(() => expect(screen.getByRole("radio", { name: "pausedDag.drain" })).toBeChecked());
    fireEvent.click(screen.getByTestId("trigger-dag-submit"));
    await waitFor(() => expect(DagRunService.triggerDagRun).toHaveBeenCalledOnce());
    fireEvent.click(screen.getByText("pausedDag.keepPaused"));
    await waitFor(() => expect(screen.getByRole("radio", { name: "pausedDag.keepPaused" })).toBeChecked());
    expect(screen.getByTestId("trigger-dag-submit")).toBeDisabled();
    act(() => {
      resolveRun?.({ dag_id: DAG_ID, dag_run_id: "manual__run" } as TriggerDagRunResponse);
    });
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
  });

  it("recovers from a denied drain by choosing Keep paused without reopening", async () => {
    const detail = "Draining requires permission to edit Dag: paused_dag";

    vi.mocked(DagRunService.triggerDagRun).mockRejectedValueOnce({ body: { detail }, status: 403 });
    const onClose = vi.fn();

    renderModal(createQueryClient(), onClose);
    fireEvent.click(await screen.findByText("pausedDag.drain"));
    await waitFor(() => expect(screen.getByRole("radio", { name: "pausedDag.drain" })).toBeChecked());
    fireEvent.click(screen.getByTestId("trigger-dag-submit"));
    expect(await screen.findByText(detail)).toBeVisible();
    expect(screen.getByTestId("trigger-dag-submit")).toBeDisabled();

    fireEvent.click(screen.getByText("pausedDag.keepPaused"));
    await waitFor(() => expect(screen.getByTestId("trigger-dag-submit")).toBeEnabled());
    expect(screen.queryByText(detail)).not.toBeInTheDocument();
    fireEvent.click(screen.getByTestId("trigger-dag-submit"));
    await waitFor(() => expect(onClose).toHaveBeenCalledOnce());
    expect(DagRunService.triggerDagRun).toHaveBeenLastCalledWith(
      expect.objectContaining({ requestBody: expect.objectContaining({ drain_dag: false }) as unknown }),
    );
  });
});
