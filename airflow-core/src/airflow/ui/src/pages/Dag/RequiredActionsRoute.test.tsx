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
import { setupServer, type SetupServer } from "msw/node";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { handlers } from "src/mocks/handlers";
import { AppWrapper } from "src/utils/AppWrapper";

const hitlRequests: Array<{ dagId: string; dagRunId: string }> = [];

let server: SetupServer;

beforeAll(() => {
  server = setupServer(
    ...handlers,
    http.get("/api/v2/dags/:dagId/dagRuns/:dagRunId/hitlDetails", ({ params }) => {
      hitlRequests.push({ dagId: String(params.dagId), dagRunId: String(params.dagRunId) });

      return HttpResponse.json({ hitl_details: [], total_entries: 0 });
    }),
  );
  server.listen({ onUnhandledFrame: "bypass" });
});
afterAll(() => server.close());

describe("Dag required actions route", () => {
  it("opens the HITL review modal scoped to the Dag", async () => {
    render(<AppWrapper initialEntries={["/dags/tutorial_taskflow_api/required_actions"]} />);

    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    await waitFor(() =>
      expect(hitlRequests).toContainEqual({ dagId: "tutorial_taskflow_api", dagRunId: "~" }),
    );
  });
});
