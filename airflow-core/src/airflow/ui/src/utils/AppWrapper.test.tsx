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
import { describe, expect, it, vi } from "vitest";

import { AppWrapper, preloadAppRoutes } from "./AppWrapper";

describe("AppWrapper", () => {
  it("refuses to render a lazy route that was not preloaded", () => {
    // React logs the error a component throws during render before rethrowing it.
    vi.spyOn(console, "error").mockImplementation(() => undefined);

    expect(() => render(<AppWrapper initialEntries={["/dags"]} />)).toThrow(
      'Add `await preloadAppRoutes(["/dags"]);` at the top of the test file.',
    );
  });

  it("renders a route once it is preloaded", async () => {
    await preloadAppRoutes(["/dags"]);
    render(<AppWrapper initialEntries={["/dags"]} />);

    expect(await screen.findByText("tutorial_taskflow_api_success")).toBeInTheDocument();
  });
});
