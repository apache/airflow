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
import type { TFunction } from "i18next";
import { afterEach, describe, expect, it, vi } from "vitest";

import { toaster } from "src/system-components";

import { createErrorToaster } from "./errorHandling";

const translate = ((key: string) => key) as unknown as TFunction;

const showToast = (error: unknown) => {
  const createToast = vi.spyOn(toaster, "create").mockImplementation(() => "toast-id");

  createErrorToaster(error, { titleKey: "dags:runAndTaskActions.delete.error" }, translate);

  return createToast;
};

describe("createErrorToaster", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it.each([
    {
      detail: "The DagRun with dag_id: `dag` and run_id: `run` cannot be deleted in running state",
      expected: "The DagRun with dag_id: `dag` and run_id: `run` cannot be deleted in running state",
      name: "a string",
    },
    {
      detail: [{ loc: ["body", "note"], msg: "Field required", type: "missing" }],
      expected: "body.note Field required",
      name: "validation entries",
    },
    {
      detail: { reason: "already running" },
      expected: "reason: already running",
      name: "a mapping",
    },
  ])("shows the API detail given as $name instead of the bare status text", ({ detail, expected }) => {
    const createToast = showToast({ body: { detail }, message: "Conflict", status: 409 });

    expect(createToast).toHaveBeenCalledWith({
      description: expected,
      title: "dags:runAndTaskActions.delete.error",
      type: "error",
    });
  });

  it.each([
    { error: { message: "Internal Server Error", status: 500 }, expected: "Internal Server Error" },
    { error: { body: "Bad Gateway", message: "Bad Gateway", status: 502 }, expected: "Bad Gateway" },
    { error: new Error("Network Error"), expected: "Network Error" },
  ])("falls back to the error message without an API detail: $expected", ({ error, expected }) => {
    const createToast = showToast(error);

    expect(createToast).toHaveBeenCalledWith(expect.objectContaining({ description: expected }));
  });

  it("leaves a 403 to the MutationCache handler", () => {
    const createToast = showToast({ body: { detail: "Forbidden" }, message: "Forbidden", status: 403 });

    expect(createToast).not.toHaveBeenCalled();
  });
});
