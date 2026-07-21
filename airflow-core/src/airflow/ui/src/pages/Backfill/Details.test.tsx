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
import { render, screen } from "@testing-library/react";
import type * as ReactRouterDom from "react-router-dom";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { BackfillResponse } from "openapi/requests/types.gen";
import { Wrapper } from "src/utils/Wrapper";

import { Details } from "./Details";

const mocks = vi.hoisted(() => ({
  useOutletContext: vi.fn(),
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string) =>
      ({
        "components:backfill.missingAndErroredRuns": "Missing and Errored Runs",
        dagId: "Dag ID",
        "dagRun.conf": "Conf",
      })[key] ?? key,
  }),
}));

vi.mock("src/components/RenderedJsonField", () => ({
  default: ({ content }: { readonly content: object }) => <span>{JSON.stringify(content)}</span>,
}));

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return { ...actual, useOutletContext: mocks.useOutletContext };
});

vi.mock("src/queries/useConfig", () => ({
  useConfig: () => undefined,
}));

const backfill: BackfillResponse = {
  completed_at: null,
  created_at: "2026-07-01T00:00:00Z",
  dag_display_name: "Example Dag",
  dag_id: "example_dag",
  dag_run_conf: { mode: "full" },
  from_date: "2026-07-01T00:00:00Z",
  id: 7,
  is_paused: false,
  max_active_runs: 4,
  reprocess_behavior: "failed",
  to_date: "2026-07-05T00:00:00Z",
  updated_at: "2026-07-01T00:00:00Z",
};

describe("Backfill details", () => {
  beforeEach(() => mocks.useOutletContext.mockReturnValue(backfill));

  it("renders metadata and Dag run configuration", () => {
    render(<Details />, { wrapper: Wrapper });

    expect(screen.getByText("example_dag")).toBeInTheDocument();
    expect(screen.getByText("Dag ID")).toBeInTheDocument();
    expect(screen.getByText("Missing and Errored Runs")).toBeInTheDocument();
    expect(screen.getByText("Conf")).toBeInTheDocument();
    expect(screen.getByText(/full/u)).toBeInTheDocument();
    expect(screen.getByText("—")).toBeInTheDocument();
  });

  it("omits the configuration row when none was provided", () => {
    mocks.useOutletContext.mockReturnValue({ ...backfill, dag_run_conf: null });

    render(<Details />, { wrapper: Wrapper });

    expect(screen.queryByText("Conf")).not.toBeInTheDocument();
  });
});
