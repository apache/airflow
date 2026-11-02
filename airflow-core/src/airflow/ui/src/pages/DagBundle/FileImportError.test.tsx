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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it, vi } from "vitest";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { FileImportError } from "./FileImportError";

const renderFor = (hasImportError: boolean) =>
  render(
    // The app does not retry a 4xx; the shared wrapper's client would, and outlast the assertion.
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <FileImportError
        bundleName="dags-folder"
        hasImportError={hasImportError}
        onClose={vi.fn()}
        relativeFileloc="broken.py"
      />
    </QueryClientProvider>,
    { wrapper: Wrapper },
  );

describe("FileImportError", () => {
  it.each([
    { expected: "dagBundles.files.importErrorForbidden", hasImportError: false, status: 403 },
    { expected: "dagBundles.files.importErrorForbidden", hasImportError: true, status: 200 },
    { expected: "dagBundles.files.importErrorGone", hasImportError: false, status: 200 },
  ])(
    "says $expected when the lookup returns $status and the file still has an error: $hasImportError",
    async ({ expected, hasImportError, status }) => {
      server.use(
        http.get("/api/v2/importErrors", () =>
          status === 200
            ? HttpResponse.json({ import_errors: [], total_entries: 0 })
            : HttpResponse.json({ detail: "Forbidden" }, { status }),
        ),
      );

      renderFor(hasImportError);

      // i18n resources are not loaded in unit tests, so the message renders as its key.
      expect(await screen.findByText(expected)).toBeInTheDocument();
    },
  );
});
