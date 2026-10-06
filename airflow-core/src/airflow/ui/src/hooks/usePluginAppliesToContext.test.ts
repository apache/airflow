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
import { createElement, type ReactNode } from "react";

import { renderHook } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import {
  UseDagServiceGetDagKeyFn,
  UseTaskInstanceServiceGetMappedTaskInstanceKeyFn,
  UseTaskServiceGetTaskKeyFn,
} from "openapi/queries";

import { usePluginAppliesToContext } from "./usePluginAppliesToContext";

const dagId = "etl_sales";
const runId = "manual__2026-01-01";
const taskId = "train_model";

let mockParams: Record<string, string> = {};

vi.mock("react-router-dom", async (importOriginal) => ({
  ...(await importOriginal<typeof ReactRouterDom>()),
  useParams: () => mockParams,
}));

const { calls, record } = vi.hoisted(() => {
  const captured: Record<string, { key: unknown; options?: { enabled?: boolean }; params: unknown }> = {};

  return {
    calls: captured,
    record: (name: string) => (params: unknown, key: unknown, options?: { enabled?: boolean }) => {
      captured[name] = { key, options, params };

      return { data: undefined, isLoading: false };
    },
  };
});

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useDagServiceGetDag: record("dag"),
  useTaskInstanceServiceGetMappedTaskInstance: record("taskInstance"),
  useTaskServiceGetTask: record("task"),
}));

const renderContext = (enabled: boolean, search = "") =>
  renderHook(() => usePluginAppliesToContext(enabled), {
    wrapper: ({ children }: { readonly children: ReactNode }) =>
      createElement(MemoryRouter, { initialEntries: [`/${search}`] }, children),
  });

describe("usePluginAppliesToContext", () => {
  beforeEach(() => {
    mockParams = { dagId, mapIndex: "-1", runId, taskId };
  });

  it("issues no query when no view needs scoping", () => {
    renderContext(false);

    expect(calls.dag?.options?.enabled).toBe(false);
    expect(calls.task?.options?.enabled).toBe(false);
    expect(calls.taskInstance?.options?.enabled).toBe(false);
  });

  it("enables every query a full task instance route can resolve", () => {
    renderContext(true);

    expect(calls.dag?.options?.enabled).toBe(true);
    expect(calls.task?.options?.enabled).toBe(true);
    expect(calls.taskInstance?.options?.enabled).toBe(true);
  });

  // Passing no explicit queryKey is what shares the page's cache entry: the generated hook
  // derives the key from the params via the same Use*KeyFn the pages go through. Asserting
  // both here means a param drifting on either side fails loudly instead of quietly
  // splitting the cache and issuing a second request.
  it("shares the pages' query keys, so each read is a cache hit", () => {
    renderContext(true);

    expect(calls.dag?.key).toBeUndefined();
    expect(calls.task?.key).toBeUndefined();
    expect(calls.taskInstance?.key).toBeUndefined();

    expect(UseDagServiceGetDagKeyFn(calls.dag?.params as { dagId: string })).toStrictEqual(
      UseDagServiceGetDagKeyFn({ dagId }),
    );
    expect(
      UseTaskServiceGetTaskKeyFn(calls.task?.params as { dagId: string; taskId: unknown }),
    ).toStrictEqual(UseTaskServiceGetTaskKeyFn({ dagId, taskId }));
    expect(
      UseTaskInstanceServiceGetMappedTaskInstanceKeyFn(
        calls.taskInstance?.params as {
          dagId: string;
          dagRunId: string;
          mapIndex: number;
          taskId: string;
        },
      ),
    ).toStrictEqual(
      UseTaskInstanceServiceGetMappedTaskInstanceKeyFn({
        dagId,
        dagRunId: runId,
        mapIndex: -1,
        taskId,
      }),
    );
  });

  it("addresses the task instance by the region coordinates in the URL", () => {
    renderContext(true, "?region_id=22222222-2222-4222-8222-222222222222&region_index=2");

    expect(calls.taskInstance?.params).toMatchObject({
      regionId: "22222222-2222-4222-8222-222222222222",
      regionIndex: 2,
    });
  });

  it("skips the task queries on a task group route, where groupId is not a task_id", () => {
    mockParams = { dagId, groupId: "my_group", runId };

    renderContext(true);

    expect(calls.dag?.options?.enabled).toBe(true);
    expect(calls.task?.options?.enabled).toBe(false);
    expect(calls.taskInstance?.options?.enabled).toBe(false);
  });

  it("skips the task instance query when mapIndex is not a number", () => {
    mockParams = { dagId, mapIndex: "not-a-number", runId, taskId };

    renderContext(true);

    expect(calls.taskInstance?.options?.enabled).toBe(false);
  });

  it("resolves only the Dag on a Dag-level route", () => {
    mockParams = { dagId };

    renderContext(true);

    expect(calls.dag?.options?.enabled).toBe(true);
    expect(calls.task?.options?.enabled).toBe(false);
    expect(calls.taskInstance?.options?.enabled).toBe(false);
  });
});
