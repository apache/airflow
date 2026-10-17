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
import { render, waitFor } from "@testing-library/react";
import { createMemoryRouter, RouterProvider } from "react-router-dom";
import { describe, expect, it } from "vitest";

import { LegacyTreeRedirect } from "src/pages/LegacyTreeRedirect";

describe("LegacyTreeRedirect", () => {
  it("redirects legacy Dag links to the Dag overview", async () => {
    const router = createMemoryRouter(
      [
        { element: <LegacyTreeRedirect />, path: "/tree" },
        { element: <div>Dag overview</div>, path: "/dags/:dagId" },
      ],
      { initialEntries: ["/tree?dag_id=example_dag"] },
    );

    render(<RouterProvider router={router} />);

    await waitFor(() => {
      expect(router.state.location.pathname).toBe("/dags/example_dag");
    });
  });

  it("redirects legacy links without a Dag ID to the Dag list", async () => {
    const router = createMemoryRouter(
      [
        { element: <LegacyTreeRedirect />, path: "/tree" },
        { element: <div>Dag list</div>, path: "/dags" },
      ],
      { initialEntries: ["/tree"] },
    );

    render(<RouterProvider router={router} />);

    await waitFor(() => {
      expect(router.state.location.pathname).toBe("/dags");
    });
  });
});
