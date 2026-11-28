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
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import "src/i18n/config";
import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { FailingTriggers } from "./FailingTriggers";

const serveTriggers = (response: Response) => {
  const requested: Array<{ limit: string | null; status: Array<string> }> = [];

  server.use(
    http.get("/api/v2/triggers", ({ request }) => {
      const params = new URL(request.url).searchParams;

      requested.push({ limit: params.get("limit"), status: params.getAll("status") });

      return response;
    }),
  );

  return requested;
};

// Tests render i18n keys: the msw handlers serve empty translation bundles.
describe("FailingTriggers", () => {
  it("counts the failing triggers and links to them", async () => {
    const requested = serveTriggers(HttpResponse.json({ total_entries: 3, triggers: [] }));

    render(<FailingTriggers />, { wrapper: Wrapper });

    expect(await screen.findByTestId("stats-card")).toHaveTextContent("3stats.failingTriggers");
    expect(screen.getByRole("link")).toHaveAttribute("href", "/triggers?status=failing");
    expect(requested).toEqual([{ limit: "1", status: ["failing"] }]);
  });

  it.each([
    { name: "there are none", response: HttpResponse.json({ total_entries: 0, triggers: [] }) },
    {
      name: "the user can see no trigger",
      response: HttpResponse.json({ detail: "Forbidden" }, { status: 403 }),
    },
  ])("shows nothing when $name", async ({ response }) => {
    const requested = serveTriggers(response);

    render(<FailingTriggers />, { wrapper: Wrapper });

    await waitFor(() => expect(requested).toHaveLength(1));
    expect(screen.queryByTestId("stats-card")).not.toBeInTheDocument();
  });
});
