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
import { render } from "@testing-library/react";
import type * as ReactI18Next from "react-i18next";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type * as SrcUtils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { HITLTaskInstances } from "./HITLTaskInstances";

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------
// `useSearchParams` is replaced with a function that returns a mutable
// reference; tests update `mockSearchParams` before each `render` call.
let mockSearchParams = new URLSearchParams();

vi.mock("react-i18next", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactI18Next>();

  return {
    ...actual,
    useTranslation: () => ({
      // eslint-disable-next-line id-length
      t: (key: string) => key,
    }),
  };
});

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return {
    ...actual,
    useParams: () => ({}),
    useSearchParams: () => [mockSearchParams, vi.fn()] as const,
  };
});

// Only `useTaskInstanceServiceGetHitlDetails` is overridden — the rest of the
// generated query module is preserved so unrelated hooks (e.g. `useConfig`'s
// `useConfigServiceGetConfigs`) keep working through the global MSW handlers.
vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useTaskInstanceServiceGetHitlDetails: vi.fn(),
  };
});

// `useAutoRefresh` is the only `src/utils` export that needs a stable, definite
// return value for the refetch predicate test. Other exports are passed through
// untouched.
vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof SrcUtils>();

  return {
    ...actual,
    useAutoRefresh: () => 5000,
  };
});

// Children of HITLTaskInstances are stubbed: the tests are about which params
// the page sends to the listing API, not how the table or filter bar render.
vi.mock("./HITLFilters", () => ({
  HITLFilters: () => null,
}));

vi.mock("src/components/DataTable", () => ({
  DataTable: () => null,
}));

const { useTaskInstanceServiceGetHitlDetails } = await import("openapi/queries");

const emptyHitlResponse = {
  data: { hitl_details: [], total_entries: 0 },
  error: null,
  isLoading: false,
};

const lastListingCall = () => {
  const { calls } = vi.mocked(useTaskInstanceServiceGetHitlDetails).mock;

  return calls.at(-1);
};

beforeEach(() => {
  vi.mocked(useTaskInstanceServiceGetHitlDetails).mockReturnValue(
    emptyHitlResponse as ReturnType<typeof useTaskInstanceServiceGetHitlDetails>,
  );
});

// ---------------------------------------------------------------------------
// Regression tests for #66428 — the listing previously hard-coded
// `mapIndex: parseInt(searchParams.get(MAP_INDEX) ?? "-1", 10)`, which silently
// dropped every mapped HITL row. The fix only sends `mapIndex` when the user
// has explicitly set the URL search param.
// ---------------------------------------------------------------------------
describe("HITLTaskInstances – mapIndex URL param handling (#66428)", () => {
  it("does not send mapIndex when map_index URL param is absent", () => {
    mockSearchParams = new URLSearchParams();

    render(<HITLTaskInstances />, { wrapper: Wrapper });

    const args = lastListingCall()?.[0] as { mapIndex?: number } | undefined;

    expect(args?.mapIndex).toBeUndefined();
  });

  it("sends mapIndex=2 when map_index=2 is set in the URL", () => {
    mockSearchParams = new URLSearchParams("map_index=2");

    render(<HITLTaskInstances />, { wrapper: Wrapper });

    const args = lastListingCall()?.[0] as { mapIndex?: number } | undefined;

    expect(args?.mapIndex).toBe(2);
  });

  it("sends mapIndex=-1 when the user explicitly filters on non-mapped tasks", () => {
    mockSearchParams = new URLSearchParams("map_index=-1");

    render(<HITLTaskInstances />, { wrapper: Wrapper });

    const args = lastListingCall()?.[0] as { mapIndex?: number } | undefined;

    expect(args?.mapIndex).toBe(-1);
  });
});

// ---------------------------------------------------------------------------
// Refetch predicate — the API serializes `responded_at` as JSON `null`, not an
// omitted field, so `=== undefined` never matched and the page never polled
// for new pending actions.
// ---------------------------------------------------------------------------
describe("HITLTaskInstances – auto-refresh predicate", () => {
  type RefetchPredicate = (query: {
    state: {
      data?: {
        hitl_details: Array<{
          responded_at: string | null;
          task_instance: { state: string };
        }>;
      };
    };
  }) => number | false;

  const getRefetchInterval = (): RefetchPredicate => {
    const args = lastListingCall();
    const options = args?.[2] as { refetchInterval?: RefetchPredicate } | undefined;

    if (options?.refetchInterval === undefined) {
      throw new Error("refetchInterval predicate not registered on the listing query");
    }

    return options.refetchInterval;
  };

  it("triggers refetch when a deferred row has responded_at: null", () => {
    mockSearchParams = new URLSearchParams();

    render(<HITLTaskInstances />, { wrapper: Wrapper });

    const refetchInterval = getRefetchInterval();
    const result = refetchInterval({
      state: {
        data: {
          hitl_details: [
            {
              responded_at: null,
              task_instance: { state: "deferred" },
            },
          ],
        },
      },
    });

    expect(result).toBe(5000);
  });

  it("does not refetch when every row already has a responded_at value", () => {
    mockSearchParams = new URLSearchParams();

    render(<HITLTaskInstances />, { wrapper: Wrapper });

    const refetchInterval = getRefetchInterval();
    const result = refetchInterval({
      state: {
        data: {
          hitl_details: [
            {
              responded_at: "2026-05-05T10:00:00Z",
              task_instance: { state: "deferred" },
            },
          ],
        },
      },
    });

    expect(result).toBe(false);
  });
});
