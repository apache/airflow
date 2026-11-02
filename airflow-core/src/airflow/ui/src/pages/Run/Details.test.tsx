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
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";

import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import { Details } from "./Details";

const getDagRun = vi.hoisted(() => vi.fn<() => DAGRunResponse>());

vi.mock("openapi/queries", async (importOriginal) => {
  const actual = await importOriginal<typeof OpenapiQueries>();

  return {
    ...actual,
    useDagRunServiceGetDagRun: () => ({ data: getDagRun() }),
    useDagRunServiceGetDagRunStats: () => ({ data: undefined }),
  };
});

vi.mock("src/hooks/useShowTeam", () => ({ useShowTeam: () => false }));
vi.mock("src/utils", async (importOriginal) => {
  const actual = await importOriginal<typeof Utils>();

  return { ...actual, useAutoRefresh: () => false };
});
vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string) => key,
  }),
}));

describe("Run Details timeout", () => {
  it.each(["PT1800S", null])("shows only an explicit run timeout: %s", (dagRunTimeout) => {
    getDagRun.mockReturnValue({
      bundle_version: null,
      conf: {},
      dag_id: "example_dag",
      dag_run_id: "manual__example",
      dag_run_timeout: dagRunTimeout,
      dag_versions: [],
      partition_date: null,
      run_type: "manual",
      state: "running",
    } as unknown as DAGRunResponse);

    render(<Details />, { wrapper: Wrapper });

    if (dagRunTimeout === null) {
      expect(screen.queryByText("dagDetails.dagRunTimeout")).not.toBeInTheDocument();
    } else {
      const row = screen.getByRole("row", { name: /dagDetails.dagRunTimeout/u });

      expect(within(row).getByText("30m")).toBeVisible();
    }
  });
});
