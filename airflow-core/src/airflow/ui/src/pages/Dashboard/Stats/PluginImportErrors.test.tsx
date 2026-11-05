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

import "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import { PluginImportErrors } from "./PluginImportErrors";

const { mockUsePluginServiceImportErrors } = vi.hoisted(() => ({
  mockUsePluginServiceImportErrors: vi.fn(),
}));

vi.mock("openapi/queries", async (importOriginal) => {
  // eslint-disable-next-line @typescript-eslint/consistent-type-imports -- `import()` type is the standard pattern for typing `importOriginal` in Vitest mocks.
  const actual = await importOriginal<typeof import("openapi/queries")>();

  return { ...actual, usePluginServiceImportErrors: mockUsePluginServiceImportErrors };
});

const failedQuery = (status: number) => ({
  error: Object.assign(new Error("Request failed"), { status }),
  isLoading: false,
});

describe("PluginImportErrors", () => {
  it("reports a failed load instead of hiding it", () => {
    mockUsePluginServiceImportErrors.mockReturnValue(failedQuery(500));

    render(
      <Wrapper>
        <PluginImportErrors />
      </Wrapper>,
    );

    expect(screen.getByTestId("error-alert")).toHaveTextContent("500 Request failed");
  });
});
