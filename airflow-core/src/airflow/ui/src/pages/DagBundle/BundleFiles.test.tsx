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
import { http, HttpResponse } from "msw";
import { describe, expect, it } from "vitest";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { BundleFiles } from "./BundleFiles";

const fileRow = (relativeFileloc: string, canReadImportError: boolean) => ({
  can_read_import_error: canReadImportError,
  dag_count: 1,
  import_error_count: 1,
  last_parse_duration: null,
  last_parsed_time: null,
  relative_fileloc: relativeFileloc,
});

describe("BundleFiles import error column", () => {
  it("offers the error only where the caller may read it", async () => {
    server.use(
      http.get("/api/v2/dagBundles/:bundleName/files", () =>
        HttpResponse.json({
          dag_bundle_files: [fileRow("readable.py", true), fileRow("hidden.py", false)],
          total_entries: 2,
        }),
      ),
    );

    render(<BundleFiles bundleName="my-bundle" />, { wrapper: Wrapper });

    expect(await screen.findByText("hidden.py")).toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "browse:dagBundles.files.showImportError" })).toHaveLength(
      1,
    );
    // The unreadable file still shows that it is broken, as a count rather than a control.
    const hiddenRow = screen.getByText("hidden.py").closest("tr");

    expect(hiddenRow).not.toBeNull();
    expect(hiddenRow?.querySelector("button")).toBeNull();
    expect(hiddenRow).toHaveTextContent("1");
  });
});
