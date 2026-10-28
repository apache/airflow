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
import type { PropsWithChildren } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { afterEach, expect, it, vi } from "vitest";

import {
  useConnectionServiceGetConnectionsKey,
  usePoolServiceGetPoolsKey,
  useVariableServiceGetVariablesKey,
} from "openapi/queries";

import { toaster } from "src/system-components";

import { server } from "src/mocks/server";
import * as errorHandling from "src/utils/errorHandling";

import { useDeleteConnection } from "./useDeleteConnection";
import { useDeletePool } from "./useDeletePool";
import { useDeleteVariable } from "./useDeleteVariable";

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, params?: { resourceName?: string }) =>
      params?.resourceName === undefined ? key : `${key}:${params.resourceName}`,
  }),
}));

afterEach(() => vi.restoreAllMocks());

const resources = [
  {
    name: "connections",
    queryKey: useConnectionServiceGetConnectionsKey,
    resource: "connections.connection_one",
  },
  { name: "pools", queryKey: usePoolServiceGetPoolsKey, resource: "pools.pool_one" },
  { name: "variables", queryKey: useVariableServiceGetVariablesKey, resource: "variables.variable_one" },
] as const;

const renderDelete = (name: (typeof resources)[number]["name"], onSuccessConfirm: () => void) => {
  const queryClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
  const hook = renderHook(
    () => {
      const connection = useDeleteConnection({ onSuccessConfirm });
      const pool = useDeletePool({ onSuccessConfirm });
      const variable = useDeleteVariable({ onSuccessConfirm });

      return {
        connections: () => connection.mutateAsync({ connectionId: "item" }),
        pools: () => pool.mutateAsync({ poolName: "item" }),
        variables: () => variable.mutateAsync({ variableKey: "item" }),
      }[name];
    },
    { wrapper },
  );

  return { ...hook, queryClient };
};

it.each(resources)(
  "waits for $name invalidation before success feedback and closing",
  async ({ name, queryKey, resource }) => {
    let requests = 0;

    server.use(
      http.delete(`*/api/v2/${name}/item`, () => {
        requests += 1;

        return new HttpResponse(null, { status: 204 });
      }),
    );
    const events: Array<string> = [];
    const close = vi.fn(() => events.push("close"));
    const toast = vi.spyOn(toaster, "create").mockImplementation(() => {
      events.push("toast");

      return "toast";
    });
    const { queryClient, result } = renderDelete(name, close);
    let finishInvalidation!: () => void;
    const invalidation = new Promise<void>((resolve) => {
      finishInvalidation = resolve;
    });
    const invalidate = vi.spyOn(queryClient, "invalidateQueries").mockImplementation(() => {
      events.push("invalidate");

      return invalidation;
    });
    let mutation!: Promise<void>;

    act(() => {
      mutation = result.current();
    });
    await waitFor(() => expect(invalidate).toHaveBeenCalledWith({ queryKey: [queryKey] }));
    expect(requests).toBe(1);
    expect(toast).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
    await act(async () => {
      finishInvalidation();
      await mutation;
    });
    expect(events).toEqual(["invalidate", "toast", "close"]);
    expect(toast).toHaveBeenCalledWith({
      description: `common:toaster.delete.success.description:admin:${resource}`,
      title: `common:toaster.delete.success.title:admin:${resource}`,
      type: "success",
    });
    expect(close).toHaveBeenCalledOnce();
  },
);

it.each(resources)(
  "preserves the $name API error without invalidating or closing",
  async ({ name, resource }) => {
    server.use(
      http.delete(`*/api/v2/${name}/item`, () =>
        HttpResponse.json({ detail: "cannot delete" }, { status: 400 }),
      ),
    );
    const close = vi.fn();
    const toast = vi.spyOn(toaster, "create").mockReturnValue("toast");
    const errorToast = vi.spyOn(errorHandling, "createErrorToaster");
    const { queryClient, result } = renderDelete(name, close);
    const invalidate = vi.spyOn(queryClient, "invalidateQueries");
    let requestError: unknown;

    await act(async () => {
      requestError = await result.current().catch((error: unknown) => error);
    });
    expect(requestError).toMatchObject({ body: { detail: "cannot delete" }, status: 400 });
    expect(errorToast).toHaveBeenCalledWith(
      requestError,
      {
        params: { resourceName: `admin:${resource}` },
        titleKey: "common:toaster.delete.error",
      },
      expect.any(Function),
    );
    expect(toast).toHaveBeenCalledWith({
      description: "Bad Request",
      title: `common:toaster.delete.error:admin:${resource}`,
      type: "error",
    });
    expect(invalidate).not.toHaveBeenCalled();
    expect(close).not.toHaveBeenCalled();
  },
);
