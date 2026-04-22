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
import { describe, expect, it } from "vitest";

import type { ImportErrorResponse } from "openapi/requests/types.gen";

import { selectLatestMatchingImportError } from "./selectLatestMatchingImportError";

const row = (
  overrides: Partial<ImportErrorResponse> & Pick<ImportErrorResponse, "import_error_id" | "timestamp">,
): ImportErrorResponse => ({
  bundle_name: "dags-folder",
  filename: "path/to/dag.py",
  stack_trace: "err",
  ...overrides,
});

describe("selectLatestMatchingImportError", () => {
  it("returns undefined when the list is empty", () => {
    expect(selectLatestMatchingImportError(undefined, "path/to/dag.py", "dags-folder")).toBeUndefined();
    expect(selectLatestMatchingImportError([], "path/to/dag.py", "dags-folder")).toBeUndefined();
  });

  it("matches exact filename and bundle", () => {
    const errors = [
      row({
        bundle_name: "dags-folder",
        filename: "other.py",
        import_error_id: 1,
        stack_trace: "wrong file",
        timestamp: "2024-01-01T00:00:00Z",
      }),
      row({
        bundle_name: "dags-folder",
        filename: "path/to/dag.py",
        import_error_id: 2,
        stack_trace: "match",
        timestamp: "2024-01-02T00:00:00Z",
      }),
    ];

    const picked = selectLatestMatchingImportError(errors, "path/to/dag.py", "dags-folder");

    expect(picked?.stack_trace).toBe("match");
  });

  it("treats null bundle the same on both sides", () => {
    const errors = [
      row({
        bundle_name: null,
        filename: "path/to/dag.py",
        import_error_id: 1,
        stack_trace: "ok",
        timestamp: "2024-01-01T00:00:00Z",
      }),
    ];

    expect(selectLatestMatchingImportError(errors, "path/to/dag.py", null)?.stack_trace).toBe("ok");
  });

  it("returns the row with the latest timestamp when several match", () => {
    const errors = [
      row({
        bundle_name: "dags-folder",
        filename: "path/to/dag.py",
        import_error_id: 1,
        stack_trace: "older",
        timestamp: "2024-01-01T00:00:00Z",
      }),
      row({
        bundle_name: "dags-folder",
        filename: "path/to/dag.py",
        import_error_id: 2,
        stack_trace: "newer",
        timestamp: "2024-06-01T00:00:00Z",
      }),
    ];

    expect(selectLatestMatchingImportError(errors, "path/to/dag.py", "dags-folder")?.stack_trace).toBe(
      "newer",
    );
  });

  it("returns undefined when bundle differs", () => {
    const errors = [
      row({
        bundle_name: "other-bundle",
        filename: "path/to/dag.py",
        import_error_id: 1,
        stack_trace: "wrong bundle",
        timestamp: "2024-01-01T00:00:00Z",
      }),
    ];

    expect(selectLatestMatchingImportError(errors, "path/to/dag.py", "dags-folder")).toBeUndefined();
  });
});
