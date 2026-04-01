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
import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { Wrapper } from "src/utils/Wrapper";

import { DocsButton } from "./DocsButton";

const mockConfig: Record<string, unknown> = {
  enable_swagger_ui: true,
};

vi.mock("src/queries/useConfig", () => ({
  useConfig: (key: string) => mockConfig[key],
}));

describe("DocsButton", () => {
  beforeEach(() => {
    mockConfig.enable_swagger_ui = true;
  });

  it("hides the REST API reference when API docs are disabled", async () => {
    render(<DocsButton externalViews={[]} showAPI={false} />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: /nav.docs/iu }));

    expect(await screen.findByText("docs.documentation")).toBeInTheDocument();
    expect(screen.queryByText("docs.restApiReference")).toBeNull();
  });

  it("shows the REST API reference when API docs are enabled", async () => {
    render(<DocsButton externalViews={[]} showAPI />, { wrapper: Wrapper });

    fireEvent.click(screen.getByRole("button", { name: /nav.docs/iu }));

    expect(await screen.findByText("docs.restApiReference")).toBeInTheDocument();
  });
});
