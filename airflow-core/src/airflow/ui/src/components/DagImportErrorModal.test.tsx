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
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ImportErrorResponse } from "openapi/requests/types.gen";

import { Wrapper } from "src/utils/Wrapper";

import { DagImportErrorModal } from "./DagImportErrorModal";

const importError: ImportErrorResponse = {
  bundle_name: "dags-folder",
  file_token: "token",
  filename: "dags/broken_dag.py",
  import_error_id: 1,
  source_reference: null,
  stack_trace: 'Traceback (most recent call last):\n  File "dags/broken_dag.py", line 1',
  timestamp: "2025-01-01T00:00:00Z",
};

describe("DagImportErrorModal", () => {
  it("shows the import error metadata and stack trace in a titled dialog", () => {
    render(<DagImportErrorModal importError={importError} onClose={vi.fn()} open />, { wrapper: Wrapper });

    // i18n resources are not loaded in unit tests, so the title renders as its key.
    const dialog = screen.getByRole("dialog", { name: "importErrors.dagImportError" });

    expect(dialog).toHaveTextContent("dags-folder");
    expect(dialog).toHaveTextContent("dags/broken_dag.py");
    expect(within(dialog).getByText(/Traceback \(most recent call last\)/u)).toBeInTheDocument();
  });
});
