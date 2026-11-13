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
import { render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { BackfillInProgressBadge } from "./BackfillInProgressBadge";

const { mockBackfills } = vi.hoisted(() => ({
  mockBackfills: { current: [] as Array<{ completed_at: string | null }> },
}));

vi.mock("openapi/queries", () => ({
  useBackfillServiceListBackfillsUi: () => ({ data: { backfills: mockBackfills.current } }),
  useDagRunServiceGetDagRuns: () => ({ data: undefined }),
  useDagServiceGetDagDetails: () => ({ data: undefined }),
}));

vi.mock("src/queries/useConfig", () => ({ useConfig: () => undefined }));

const label = i18n.t("banner.backfillInProgress", { ns: "components" });

afterEach(() => {
  mockBackfills.current = [];
});

describe("BackfillInProgressBadge", () => {
  it("links to the Backfills tab while a backfill has not finished", () => {
    mockBackfills.current = [{ completed_at: null }];

    render(<BackfillInProgressBadge dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: label })).toHaveAttribute("href", "/dags/example_dag/backfills");
  });

  it("renders nothing once every backfill has completed", () => {
    mockBackfills.current = [{ completed_at: "2024-08-22T19:00:00+00:00" }];

    render(<BackfillInProgressBadge dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.queryByRole("link", { name: label })).not.toBeInTheDocument();
  });

  it("renders nothing when the Dag has never been backfilled", () => {
    render(<BackfillInProgressBadge dagId="example_dag" />, { wrapper: Wrapper });

    expect(screen.queryByRole("link", { name: label })).not.toBeInTheDocument();
  });
});
