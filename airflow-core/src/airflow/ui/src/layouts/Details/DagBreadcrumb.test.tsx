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
import type { ReactNode } from "react";
import type * as ReactRouterDom from "react-router-dom";
import { describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { DagBreadcrumb } from "./DagBreadcrumb";

vi.mock("openapi/queries", () => ({
  useDagRunServiceGetDagRun: () => ({ data: undefined }),
  useDagServiceGetDagDetails: () => ({ data: { dag_display_name: "Example Dag", is_stale: true } }),
  useTaskInstanceServiceGetMappedTaskInstance: () => ({ data: undefined }),
  useTaskServiceGetTask: () => ({ data: undefined }),
}));

vi.mock("react-router-dom", async (importOriginal) => {
  const actual = await importOriginal<typeof ReactRouterDom>();

  return { ...actual, useParams: () => ({ backfillId: "7", dagId: "example_dag" }) };
});

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    // eslint-disable-next-line id-length
    t: (key: string, options?: { count?: number }) => {
      if (key === "backfill") {
        return options?.count === 2 ? "Backfills" : "Backfill";
      }
      if (key === "backfill_one") {
        return "Backfill";
      }
      if (key === "dag_one") {
        return "Dag";
      }

      return key;
    },
  }),
}));

vi.mock("src/components/BreadcrumbStats", () => ({
  BreadcrumbStats: ({ links }: { readonly links: Array<{ label: ReactNode; value?: string }> }) => (
    <nav>
      {links.map((link) =>
        link.value === undefined ? (
          <span key="current">{link.label}</span>
        ) : (
          <a href={link.value} key={link.value}>
            {link.label}
          </a>
        ),
      )}
    </nav>
  ),
}));

vi.mock("src/utils", () => ({
  isStatePending: () => false,
  useAutoRefresh: () => false,
}));

describe("DagBreadcrumb", () => {
  it("links through Backfills to the selected backfill", () => {
    render(<DagBreadcrumb />, { wrapper: Wrapper });

    expect(screen.getByRole("link", { name: "Backfills" })).toHaveAttribute(
      "href",
      "/dags/example_dag/backfills",
    );
    expect(screen.getByRole("link", { name: "Backfill #7" })).toHaveAttribute(
      "href",
      "/dags/example_dag/backfills/7",
    );
  });
});
