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
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { describe, expect, it, vi } from "vitest";

import type { ImportErrorResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";
import { Wrapper } from "src/utils/Wrapper";

import { DagImportErrorsModal } from "./DagImportErrorsModal";

const importError: ImportErrorResponse = {
  bundle_name: "dags-folder",
  file_token: "token",
  filename: "dags/broken_dag.py",
  import_error_id: 1,
  source_reference: null,
  stack_trace: 'Traceback (most recent call last):\n  File "dags/broken_dag.py", line 1',
  timestamp: "2025-01-01T00:00:00Z",
};

describe("DagImportErrorsModal", () => {
  it("copies the stack trace, not the file name", async () => {
    server.use(
      http.get("/api/v2/importErrors", () =>
        HttpResponse.json({ import_errors: [importError], total_entries: 1 }),
      ),
    );
    const writeText = vi.fn<(text: string) => Promise<void>>().mockResolvedValue(undefined);

    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
    render(<DagImportErrorsModal onClose={vi.fn()} open />, { wrapper: Wrapper });

    const dialog = await screen.findByRole("dialog");

    fireEvent.click(await within(dialog).findByRole("button", { name: /copy/iu }));

    await waitFor(() => expect(writeText).toHaveBeenCalledWith(importError.stack_trace));
  });
});
