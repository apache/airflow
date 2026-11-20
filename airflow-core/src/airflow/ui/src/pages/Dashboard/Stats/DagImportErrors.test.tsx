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

import { DagImportErrors } from "./DagImportErrors";

const { mockUseImportErrorServiceGetImportErrors } = vi.hoisted(() => ({
  mockUseImportErrorServiceGetImportErrors: vi.fn(),
}));

vi.mock("openapi/queries/queries", async (importOriginal) => {
  const actual = await importOriginal();

  return {
    ...(actual as object),
    useImportErrorServiceGetImportErrors: mockUseImportErrorServiceGetImportErrors,
  };
});

describe("DagImportErrors", () => {
  it("keeps the last count without an error banner when a refetch fails", () => {
    mockUseImportErrorServiceGetImportErrors.mockReturnValue({
      data: { import_errors: [], total_entries: 3 },
      error: Object.assign(new Error("Internal Server Error"), { status: 500 }),
      isLoading: false,
    });

    render(
      <Wrapper>
        <DagImportErrors />
      </Wrapper>,
    );

    expect(screen.getByTestId("stats-card")).toHaveTextContent("3");
    expect(screen.queryByTestId("error-alert")).not.toBeInTheDocument();
  });
});
