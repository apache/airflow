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

// The assets mock handler (see src/mocks/handlers/assets.ts) serves asset 1 with one
// alias and one watcher, and asset 2 with neither.
describe("Asset header", () => {
  it("lists the aliases and watchers of an asset", async () => {
    render(<AppWrapper initialEntries={["/assets/1"]} />);

    await waitFor(() => expect(screen.getByRole("button", { name: "1 alias" })).toBeInTheDocument());

    expect(screen.getByRole("button", { name: "1 watcher" })).toBeInTheDocument();
  });

  it("omits the aliases and watchers stats for an asset that has none", async () => {
    render(<AppWrapper initialEntries={["/assets/2"]} />);

    await waitFor(() => expect(screen.getAllByText("plain_asset").length).toBeGreaterThan(0));

    expect(screen.queryByRole("button", { name: /alias/iu })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /watcher/iu })).not.toBeInTheDocument();
  });
});
