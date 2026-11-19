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

import { DagsListSearchContext } from "src/context/dagsListSearch";
import { Wrapper } from "src/utils/Wrapper";

import { DagsLayout } from "./DagsLayout";

vi.mock("src/utils", async () => {
  const actual = await vi.importActual("src/utils");

  return { ...actual, useContainerWidth: () => 1000 };
});

describe("DagsLayout", () => {
  it("links the Dags tab to the remembered list query", () => {
    render(
      <DagsListSearchContext.Provider value="tags=example">
        <DagsLayout />
      </DagsListSearchContext.Provider>,
      { wrapper: Wrapper },
    );

    expect(screen.getByRole("link", { name: "nav.dags" })).toHaveAttribute("href", "/dags?tags=example");
  });
});
