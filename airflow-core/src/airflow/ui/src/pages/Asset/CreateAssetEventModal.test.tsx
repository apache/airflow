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
import "@testing-library/jest-dom";
import { fireEvent, render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { AssetResponse, DAGDetailsResponse } from "openapi/requests/types.gen";
import type { DagRunTriggerParams } from "src/components/TriggerDag/types";
import type * as Ui from "src/components/ui";
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

vi.mock("src/components/ui", async (importOriginal) => {
  const actual = await importOriginal<typeof Ui>();
  // Must stay inside the factory: vitest hoists vi.mock above module scope, so an outer-scope
  // component cannot be referenced here.
  // eslint-disable-next-line unicorn/consistent-function-scoping
  const DialogPart = ({ children }: { readonly children?: ReactNode }) => <div>{children}</div>;

  return {
    ...actual,
    Dialog: {
      ...actual.Dialog,
      Body: DialogPart,
      CloseTrigger: () => undefined,
      Content: DialogPart,
      Footer: DialogPart,
      Header: DialogPart,
      Root: ({ children, open }: { readonly children?: ReactNode; readonly open?: boolean }) =>
        open ? <div>{children}</div> : undefined,
    },
  };
});

vi.mock("src/components/JsonEditor", () => ({
  JsonEditor: ({ value = "{}" }: { readonly value?: string }) => (
    <textarea aria-label="Extra JSON" readOnly value={value} />
  ),
}));

vi.mock("src/components/TriggerDag/TriggerDAGForm", () => ({
  default: ({ onSubmitTrigger }: { readonly onSubmitTrigger: (params: DagRunTriggerParams) => void }) => (
    <button onClick={() => onSubmitTrigger(materializeSubmitParams)} type="button">
      submit materialize
    </button>
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
  useAssetServiceMaterializeAsset,
  useDagServiceGetDagDetails,
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
    vi.mocked(useAssetServiceCreateAssetEvent).mockReturnValue({
      error: undefined,
      isPending: false,
      mutate: createAssetEvent,
    } as unknown as ReturnType<typeof useAssetServiceCreateAssetEvent>);
    vi.mocked(useAssetServiceMaterializeAsset).mockReturnValue({
      error: undefined,
      isPending: false,
      mutate: materializeAsset,
    } as unknown as ReturnType<typeof useAssetServiceMaterializeAsset>);
    vi.mocked(useDependenciesServiceGetDependencies).mockReturnValue(noUpstreamDependencies);
    vi.mocked(useDagServiceGetDagDetails).mockReturnValue({
      data: undefined,
    } as ReturnType<typeof useDagServiceGetDagDetails>);
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
