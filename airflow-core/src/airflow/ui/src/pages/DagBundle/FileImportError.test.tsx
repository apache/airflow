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
import { useState } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import "@testing-library/jest-dom/vitest";
import { render, screen } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it, vi } from "vitest";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { FileImportError } from "./FileImportError";

const FORBIDDEN = "dagBundles.files.importErrorForbidden";
const GONE = "dagBundles.files.importErrorGone";

/** Renders the dialog for a file whose row shows an error until a recheck, which a fixed file clears. */
const Harness = ({
  fixedOnRecheck,
  hasImportError,
  onRecheck,
}: {
  readonly fixedOnRecheck: boolean;
  readonly hasImportError: boolean;
  readonly onRecheck: () => void;
}) => {
  const [rowHasError, setRowHasError] = useState(hasImportError);

  return (
    <FileImportError
      bundleName="dags-folder"
      hasImportError={rowHasError}
      onClose={vi.fn()}
      recheckImportError={() => {
        onRecheck();
        if (fixedOnRecheck) {
          setRowHasError(false);
        }

        return Promise.resolve();
      }}
      relativeFileloc="broken.py"
    />
  );
};

const IMPORT_ERROR = {
  bundle_name: "dags-folder",
  filename: "broken.py",
  import_error_id: 1,
  stack_trace: "Traceback: broken again",
  timestamp: "2026-11-02T00:00:00Z",
};

const renderWith = ({
  fixedOnRecheck = false,
  hasImportError,
  reappearsOnRecheck = false,
  status,
}: {
  readonly fixedOnRecheck?: boolean;
  readonly hasImportError: boolean;
  readonly reappearsOnRecheck?: boolean;
  readonly status: number;
}) => {
  let requests = 0;

  server.use(
    http.get("/api/v2/importErrors", () => {
      requests += 1;
      if (status !== 200) {
        return HttpResponse.json({ detail: "boom" }, { status });
      }
      const importErrors = reappearsOnRecheck && requests > 1 ? [IMPORT_ERROR] : [];

      return HttpResponse.json({ import_errors: importErrors, total_entries: importErrors.length });
    }),
  );
  const onRecheck = vi.fn();

  render(
    // The app does not retry a 4xx; the shared wrapper's client would, and outlast the assertion.
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <Harness fixedOnRecheck={fixedOnRecheck} hasImportError={hasImportError} onRecheck={onRecheck} />
    </QueryClientProvider>,
    { wrapper: Wrapper },
  );

  return onRecheck;
};

// i18n resources are not loaded in unit tests, so messages render as their keys.
describe("FileImportError", () => {
  it("says forbidden on a 403 without rechecking the file list", async () => {
    const onRecheck = renderWith({ hasImportError: true, status: 403 });

    expect(await screen.findByText(FORBIDDEN)).toBeInTheDocument();
    expect(onRecheck).not.toHaveBeenCalled();
  });

  it("says forbidden when the file still counts the error after a recheck", async () => {
    const onRecheck = renderWith({ hasImportError: true, status: 200 });

    expect(await screen.findByText(FORBIDDEN)).toBeInTheDocument();
    expect(onRecheck).toHaveBeenCalledTimes(1);
  });

  it("says gone when the recheck shows the error was fixed since the last poll", async () => {
    const onRecheck = renderWith({ fixedOnRecheck: true, hasImportError: true, status: 200 });

    expect(await screen.findByText(GONE)).toBeInTheDocument();
    expect(onRecheck).toHaveBeenCalledTimes(1);
  });

  it("shows an error that came back since an empty result was fetched", async () => {
    const onRecheck = renderWith({ hasImportError: true, reappearsOnRecheck: true, status: 200 });

    expect(await screen.findByText(IMPORT_ERROR.stack_trace)).toBeInTheDocument();
    expect(onRecheck).toHaveBeenCalledTimes(1);
  });

  it("shows other failures as errors rather than as a missing permission", async () => {
    renderWith({ hasImportError: true, status: 500 });

    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    expect(screen.queryByText(FORBIDDEN)).not.toBeInTheDocument();
    expect(screen.queryByText(GONE)).not.toBeInTheDocument();
  });
});
