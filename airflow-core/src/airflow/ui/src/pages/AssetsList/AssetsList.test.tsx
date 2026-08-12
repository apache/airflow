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
import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { AppWrapper } from "src/utils/AppWrapper";

// The assets mock handler (see src/mocks/handlers/assets.ts) returns a single asset
// with one consuming task, one alias and one watcher.
describe("AssetsList columns", () => {
  it("shows the consuming tasks of an asset", async () => {
    render(<AppWrapper initialEntries={["/assets"]} />);

    await waitFor(() => expect(screen.getByText("asset_with_dependencies")).toBeInTheDocument());

    expect(screen.getByRole("button", { name: "1 task" })).toBeInTheDocument();
  });

  it("lists the aliases and watchers of an asset", async () => {
    render(<AppWrapper initialEntries={["/assets"]} />);

    await waitFor(() => expect(screen.getByRole("button", { name: "1 alias" })).toBeInTheDocument());

    expect(screen.getByRole("button", { name: "1 watcher" })).toBeInTheDocument();
  });
});
