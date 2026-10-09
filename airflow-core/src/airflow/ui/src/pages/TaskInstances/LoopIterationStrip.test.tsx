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
import { render, screen, waitFor } from "@testing-library/react";
import type * as ReactI18Next from "react-i18next";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { LoopIterationStrip } from "./LoopIterationStrip";

const mockParams = {
  dagId: "example_dag",
  groupId: "batch",
  runId: "manual__2026-06-07T00:00:00+00:00",
};
let mockSearchParams = new URLSearchParams();
let iterationCount = 3;

vi.mock("react-i18next", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactI18Next>();

  return {
    ...actual,
    useTranslation: () => ({
      i18n: { language: "en" },
      // eslint-disable-next-line id-length
      t: (key: string) => key,
    }),
  };
});

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return {
    ...actual,
    useParams: () => mockParams,
    useSearchParams: () => [mockSearchParams, vi.fn()] as const,
  };
});

vi.mock("src/queries/useIsLoopGroup", () => ({
  useLoopGroupIds: () => ["batch"],
}));

vi.mock("openapi/requests/services.gen", () => ({
  GridService: {
    getLoopSummary: () =>
      Promise.resolve({
        iterations: Array.from({ length: iterationCount }, (_, index) => ({
          index,
          start_date: null,
          state: "success",
          task_count: 2,
        })),
        iterations_ran: iterationCount,
        max_iterations: 20,
        status: "ran_to_cap",
      }),
  },
}));

describe("LoopIterationStrip", () => {
  beforeEach(() => {
    mockSearchParams = new URLSearchParams();
  });

  it("puts each pass on its own chip while they still fit", async () => {
    iterationCount = 3;
    render(<LoopIterationStrip />, { wrapper: Wrapper });

    // One chip per pass, plus the "All" chip that clears the iteration.
    await waitFor(() => expect(screen.getAllByRole("button")).toHaveLength(4));
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
  });

  it("keeps chips at exactly ten passes, the last count that fits", async () => {
    iterationCount = 10;
    render(<LoopIterationStrip />, { wrapper: Wrapper });

    await waitFor(() => expect(screen.getAllByRole("button")).toHaveLength(11));
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
  });

  it("collapses the passes into the dropdown once there are more than ten", async () => {
    iterationCount = 11;
    render(<LoopIterationStrip />, { wrapper: Wrapper });

    await waitFor(() => expect(screen.getByRole("combobox")).toBeInTheDocument());
    // The chips are gone rather than merely scrolled: nothing but the trigger remains.
    expect(screen.queryAllByRole("button")).toHaveLength(0);
  });
});
