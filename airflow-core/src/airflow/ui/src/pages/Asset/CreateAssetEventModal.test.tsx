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
import "@testing-library/jest-dom/vitest";
import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import { CancelablePromise } from "openapi/requests/core/CancelablePromise";
import { DagService } from "openapi/requests/services.gen";
import type {
  AssetEventResponse,
  AssetResponse,
  DAGDetailsResponse,
  DAGRunResponse,
} from "openapi/requests/types.gen";

import type * as Ui from "src/system-components";

import type { DagRunTriggerParams } from "src/components/TriggerDag/types";

import { Wrapper } from "src/utils/Wrapper";

import { CreateAssetEventModal } from "./CreateAssetEventModal";

const materializeSubmitParams = vi.hoisted<DagRunTriggerParams>(() => ({
  conf: "{}",
  dagRunId: "",
  dataIntervalEnd: "",
  dataIntervalMode: "auto",
  dataIntervalStart: "",
  logicalDate: "",
  note: "",
  partitionKey: undefined,
}));

vi.mock("src/system-components", async (importOriginal) => {
  const actual = await importOriginal<typeof Ui>();

  return {
    ...actual,
    Modal: ({
      children,
      footerActions,
      open,
      title,
    }: {
      readonly children?: ReactNode;
      readonly footerActions?: ReactNode;
      readonly open?: boolean;
      readonly title?: ReactNode;
    }) =>
      open ? (
        <div>
          {title}
          {children}
          {footerActions}
        </div>
      ) : undefined,
  };
});

vi.mock("src/components/JsonEditor", () => ({
  JsonEditor: ({ value = "{}" }: { readonly value?: string }) => (
    <textarea aria-label="Extra JSON" readOnly value={value} />
  ),
}));

vi.mock("src/components/TriggerDag/TriggerDAGForm", () => ({
  default: ({
    disabled,
    isPaused,
    onPausedDagActionChange,
    onSubmitTrigger,
  }: {
    readonly disabled?: boolean;
    readonly isPaused: boolean;
    readonly onPausedDagActionChange?: () => void;
    readonly onSubmitTrigger: (params: DagRunTriggerParams) => void;
  }) => (
    <>
      <span>{`paused=${String(isPaused)}`}</span>
      <button onClick={onPausedDagActionChange} type="button">
        Keep paused
      </button>
      <button disabled={disabled} onClick={() => onSubmitTrigger(materializeSubmitParams)} type="button">
        submit materialize
      </button>
    </>
  ),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useAssetServiceCreateAssetEvent: vi.fn(),
    useAssetServiceMaterializeAsset: vi.fn(),
    useDagServiceGetDagDetails: vi.fn(),
    useDependenciesServiceGetDependencies: vi.fn(),
  };
});

const {
  useAssetServiceCreateAssetEvent,
  useAssetServiceGetAssetsUiKey,
  useAssetServiceMaterializeAsset,
  useDagServiceGetDagDetails,
  UseDagServiceGetDagDetailsKeyFn,
  UseDagServiceGetDagKeyFn,
  useDependenciesServiceGetDependencies,
} = await import("openapi/queries");

const asset = {
  aliases: [],
  consuming_tasks: [],
  created_at: "2025-01-01T00:00:00Z",
  extra: {},
  group: "",
  id: 1,
  name: "my_asset",
  producing_tasks: [],
  scheduled_dags: [],
  updated_at: "2025-01-01T00:00:00Z",
  uri: "s3://bucket/my_asset",
  watchers: [],
} satisfies AssetResponse;

const createAssetEvent = vi.fn();
const materializeAsset = vi.fn();
const resetMaterializeError = vi.fn();

const noUpstreamDependencies = {
  data: { edges: [], nodes: [] },
} as ReturnType<typeof useDependenciesServiceGetDependencies>;

const withUpstreamDependencies = {
  data: {
    edges: [{ source_id: "dag:upstream_dag", target_id: `asset:${asset.id}` }],
    nodes: [],
  },
} as ReturnType<typeof useDependenciesServiceGetDependencies>;

const upstreamDag = {
  dag_display_name: "Upstream Dag",
  is_paused: false,
  timetable_partitioned: true,
  timetable_summary: null,
} as unknown as DAGDetailsResponse;

describe("CreateAssetEventModal", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    materializeSubmitParams.drainDag = undefined;
    vi.mocked(useAssetServiceCreateAssetEvent).mockReturnValue({
      error: undefined,
      isPending: false,
      mutate: createAssetEvent,
    } as unknown as ReturnType<typeof useAssetServiceCreateAssetEvent>);
    vi.mocked(useAssetServiceMaterializeAsset).mockReturnValue({
      error: undefined,
      isPending: false,
      mutate: materializeAsset,
      reset: resetMaterializeError,
    } as unknown as ReturnType<typeof useAssetServiceMaterializeAsset>);
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(noUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof useDagServiceGetDagDetails>);
  });

  it.each([true, false])(
    "waits for fresh materialization Dag state (cached paused=%s)",
    async (cachedPaused) => {
      const actual = await vi.importActual<typeof OpenapiQueries>("openapi/queries");

      vi.mocked(useDagServiceGetDagDetails).mockImplementation(actual.useDagServiceGetDagDetails);
      vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);
      let resolveDag: ((value: DAGDetailsResponse) => void) | undefined;
      const getDag = vi.spyOn(DagService, "getDagDetails").mockReturnValue(
        new CancelablePromise((resolve) => {
          resolveDag = resolve;
        }),
      );
      const queryClient = new QueryClient({
        defaultOptions: { queries: { retry: false, staleTime: Infinity } },
      });

      queryClient.setQueryData(UseDagServiceGetDagDetailsKeyFn({ dagId: "upstream_dag" }), {
        ...upstreamDag,
        is_paused: cachedPaused,
      });
      render(
        <QueryClientProvider client={queryClient}>
          <CreateAssetEventModal asset={asset} onClose={vi.fn()} open />
        </QueryClientProvider>,
        { wrapper: Wrapper },
      );
      fireEvent.click(screen.getByText("createEvent.materialize.label"));
      expect(screen.getByText("submit materialize")).toBeDisabled();
      fireEvent.click(screen.getByText("submit materialize"));
      expect(materializeAsset).not.toHaveBeenCalled();
      act(() => {
        resolveDag?.({ ...upstreamDag, is_paused: !cachedPaused });
      });
      await waitFor(() => expect(screen.getByText("submit materialize")).toBeEnabled());
      expect(screen.getByText(`paused=${String(!cachedPaused)}`)).toBeInTheDocument();
      getDag.mockRestore();
    },
  );

  it("blocks materialization after the Dag refresh fails", () => {
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: upstreamDag,
      isError: true,
      isFetching: false,
    } as ReturnType<typeof useDagServiceGetDagDetails>);
    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });
    fireEvent.click(screen.getByText("createEvent.materialize.label"));
    expect(screen.getByText("submit materialize")).toBeDisabled();
  });

  it("resets a materialization error when the paused Dag choice changes", () => {
    vi.mocked(useAssetServiceMaterializeAsset).mockReturnValue({
      error: { status: 403 },
      isPending: false,
      mutate: materializeAsset,
      reset: resetMaterializeError,
    } as unknown as ReturnType<typeof useAssetServiceMaterializeAsset>);
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: { ...upstreamDag, is_paused: true },
    } as ReturnType<typeof useDagServiceGetDagDetails>);
    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });
    fireEvent.click(screen.getByText("createEvent.materialize.label"));
    fireEvent.click(screen.getByText("Keep paused"));
    expect(resetMaterializeError).toHaveBeenCalledOnce();
  });

  it("renders the manual partition key field as a plain text Input, not the JSON editor", () => {
    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    const partitionKeyInput = screen.getByLabelText("common:dagRun.partitionKey");

    expect(partitionKeyInput.tagName).toBe("INPUT");
    expect(screen.getByLabelText("Extra JSON").tagName).toBe("TEXTAREA");
  });

  it("sends partition_key as null when the manual partition key is left empty", () => {
    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    fireEvent.click(screen.getByText("createEvent.button"));

    expect(createAssetEvent).toHaveBeenCalledWith({
      requestBody: expect.objectContaining({ partition_key: null }) as unknown,
    });
  });

  it("invalidates the assets list cache after creating an event", async () => {
    const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries").mockResolvedValue();

    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    const onSuccess = vi.mocked(useAssetServiceCreateAssetEvent).mock.calls.at(-1)?.[0]?.onSuccess as
      ((data: AssetEventResponse) => Promise<void>) | undefined;

    await onSuccess?.({ id: 1 } as unknown as AssetEventResponse);

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: [useAssetServiceGetAssetsUiKey] });
  });

  it("sends the entered manual partition key as-is", () => {
    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    fireEvent.change(screen.getByLabelText("common:dagRun.partitionKey"), {
      target: { value: "2025-01-01" },
    });
    fireEvent.click(screen.getByText("createEvent.button"));

    expect(createAssetEvent).toHaveBeenCalledWith({
      requestBody: expect.objectContaining({ partition_key: "2025-01-01" }) as unknown,
    });
  });

  it("sends materialize partition_key as null when the trigger form leaves it undefined", () => {
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: upstreamDag,
    } as ReturnType<typeof useDagServiceGetDagDetails>);
    materializeSubmitParams.partitionKey = undefined;

    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    fireEvent.click(screen.getByText("createEvent.materialize.label"));
    fireEvent.click(screen.getByText("submit materialize"));

    expect(materializeAsset).toHaveBeenCalledWith(
      expect.objectContaining({
        requestBody: expect.objectContaining({ partition_key: null }) as unknown,
      }),
    );
  });

  it("forwards the trigger form's drain choice as drain_dag", () => {
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: upstreamDag,
    } as ReturnType<typeof useDagServiceGetDagDetails>);
    materializeSubmitParams.drainDag = true;

    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    fireEvent.click(screen.getByText("createEvent.materialize.label"));
    fireEvent.click(screen.getByText("submit materialize"));

    expect(materializeAsset).toHaveBeenCalledWith(
      expect.objectContaining({
        requestBody: expect.objectContaining({ drain_dag: true }) as unknown,
      }),
    );
  });

  it("refetches the upstream Dag's paused state instead of trusting the cache", () => {
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);

    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    expect(useDagServiceGetDagDetails).toHaveBeenCalledWith(
      { dagId: "upstream_dag" },
      undefined,
      expect.objectContaining({ staleTime: 0 }),
    );
  });

  it("invalidates the upstream Dag after a materialize", async () => {
    const invalidateSpy = vi.spyOn(QueryClient.prototype, "invalidateQueries").mockResolvedValue();

    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    const onSuccess = vi.mocked(useAssetServiceMaterializeAsset).mock.calls.at(-1)?.[0]?.onSuccess as
      ((data: DAGRunResponse) => Promise<void>) | undefined;

    expect(onSuccess).toBeTypeOf("function");
    await onSuccess?.({ dag_id: "upstream_dag", dag_run_id: "materialize__run" } as DAGRunResponse);

    for (const queryKey of [
      UseDagServiceGetDagKeyFn({ dagId: "upstream_dag" }, [{ dagId: "upstream_dag" }]),
      UseDagServiceGetDagDetailsKeyFn({ dagId: "upstream_dag" }, [{ dagId: "upstream_dag" }]),
    ]) {
      expect(invalidateSpy).toHaveBeenCalledWith({ queryKey });
    }
  });

  it("sends the materialize partition_key from the trigger form as-is", () => {
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(withUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: upstreamDag,
    } as ReturnType<typeof useDagServiceGetDagDetails>);
    materializeSubmitParams.partitionKey = "2025-02-02";

    render(<CreateAssetEventModal asset={asset} onClose={vi.fn()} open />, { wrapper: Wrapper });

    fireEvent.click(screen.getByText("createEvent.materialize.label"));
    fireEvent.click(screen.getByText("submit materialize"));

    expect(materializeAsset).toHaveBeenCalledWith(
      expect.objectContaining({
        requestBody: expect.objectContaining({ partition_key: "2025-02-02" }) as unknown,
      }),
    );
  });
});
