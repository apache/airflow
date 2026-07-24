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
import { render } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type {
  AssetResponse,
  DAGDetailsResponse,
  DAGRunResponse,
  ReactAppResponse,
  TaskInstanceResponse,
} from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { ReactPlugin, type PluginProps } from "./ReactPlugin";

const mockAsset = { id: 1, name: "my_asset", uri: "s3://bucket/key" } as AssetResponse;
const mockDag = { dag_display_name: "My Dag", dag_id: "my_dag" } as DAGDetailsResponse;
const mockDagRun = { dag_id: "my_dag", dag_run_id: "run_1", state: "success" } as DAGRunResponse;
const mockTaskInstance = { dag_id: "my_dag", task_id: "my_task" } as TaskInstanceResponse;

let mockParams: Record<string, string> = {};

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useParams: () => mockParams,
}));

type QueryOptions = { enabled?: boolean };

// Each mocked hook mirrors the real gating: it returns its record only when the caller enables
// the query (i.e. the route params it depends on are present), and undefined otherwise.
vi.mock("openapi/queries", () => ({
  useAssetServiceGetAsset: (_params: unknown, _key: unknown, options?: QueryOptions) => ({
    data: options?.enabled ? mockAsset : undefined,
  }),
  useDagRunServiceGetDagRun: (_params: unknown, _key: unknown, options?: QueryOptions) => ({
    data: options?.enabled ? mockDagRun : undefined,
  }),
  useDagServiceGetDagDetails: (_params: unknown, _key: unknown, options?: QueryOptions) => ({
    data: options?.enabled ? mockDag : undefined,
  }),
  useTaskInstanceServiceGetMappedTaskInstance: (_params: unknown, _key: unknown, options?: QueryOptions) => ({
    data: options?.enabled ? mockTaskInstance : undefined,
  }),
}));

const reactApp = {
  bundle_url: "http://localhost/plugin.js",
  destination: "dag",
  name: "TestPlugin",
} as ReactAppResponse;

let capturedProps: PluginProps | undefined;

const renderPlugin = () => {
  capturedProps = undefined;
  // Pre-register the component so ReactPlugin renders it directly instead of lazy-loading a bundle.
  (globalThis as Record<string, unknown>)[reactApp.name] = (props: PluginProps) => {
    capturedProps = props;

    return null;
  };

  render(<ReactPlugin reactApp={reactApp} />, { wrapper: Wrapper });
};

describe("ReactPlugin context props", () => {
  beforeEach(() => {
    mockParams = {};
  });

  afterEach(() => {
    (globalThis as Record<string, unknown>)[reactApp.name] = undefined;
  });

  it("passes no context objects when the route has no params (base/dashboard mounts)", () => {
    renderPlugin();

    expect(capturedProps?.dag).toBeUndefined();
    expect(capturedProps?.dagRun).toBeUndefined();
    expect(capturedProps?.taskInstance).toBeUndefined();
    expect(capturedProps?.asset).toBeUndefined();
  });

  it("passes the cached Dag object when dagId is in the route", () => {
    mockParams = { dagId: "my_dag" };

    renderPlugin();

    expect(capturedProps?.dagId).toBe("my_dag");
    expect(capturedProps?.dag).toStrictEqual(mockDag);
    expect(capturedProps?.dagRun).toBeUndefined();
    expect(capturedProps?.taskInstance).toBeUndefined();
  });

  it("passes the cached DagRun object when dagId and runId are in the route", () => {
    mockParams = { dagId: "my_dag", runId: "run_1" };

    renderPlugin();

    expect(capturedProps?.dag).toStrictEqual(mockDag);
    expect(capturedProps?.dagRun).toStrictEqual(mockDagRun);
    expect(capturedProps?.taskInstance).toBeUndefined();
  });

  it("passes the cached TaskInstance object when dagId, runId and taskId are in the route", () => {
    mockParams = { dagId: "my_dag", runId: "run_1", taskId: "my_task" };

    renderPlugin();

    expect(capturedProps?.dag).toStrictEqual(mockDag);
    expect(capturedProps?.dagRun).toStrictEqual(mockDagRun);
    expect(capturedProps?.taskInstance).toStrictEqual(mockTaskInstance);
  });

  it("passes the cached Asset object and its uri when assetId is in the route", () => {
    mockParams = { assetId: "1" };

    renderPlugin();

    expect(capturedProps?.assetId).toBe("1");
    expect(capturedProps?.asset).toStrictEqual(mockAsset);
    expect(capturedProps?.assetUri).toBe(mockAsset.uri);
  });
});
