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
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ConnectionResponse } from "openapi/requests/types.gen";
import type * as ComponentsUi from "src/components/ui";
import type * as Utils from "src/utils";
import { Wrapper } from "src/utils/Wrapper";

import TestConnectionButton from "./TestConnectionButton";

const { create, mutate } = vi.hoisted(() => ({ create: vi.fn(), mutate: vi.fn() }));

let enqueueOptions: { onError: (error: unknown) => void; onSuccess: (response: { token: string }) => void };
let testStatus: { result_message?: string | null; state: string } | undefined;
let configValue = "Enabled";

vi.mock("openapi/queries", () => ({
  useConnectionServiceEnqueueConnectionTest: vi.fn((options: typeof enqueueOptions) => {
    enqueueOptions = options;

    return { isPending: false, mutate };
  }),
  useConnectionServiceGetConnectionTest: vi.fn(() => ({ data: testStatus })),
}));

vi.mock("src/queries/useConfig", () => ({
  useConfig: vi.fn(() => configValue),
}));

vi.mock("src/utils", async () => {
  const actual = await vi.importActual<typeof Utils>("src/utils");

  return { ...actual, useAutoRefresh: vi.fn(() => 2000) };
});

vi.mock("src/components/ui", async () => {
  const actual = await vi.importActual<typeof ComponentsUi>("src/components/ui");

  return { ...actual, toaster: { create } };
});

const connection = {
  conn_type: "http",
  connection_id: "my_conn",
  description: "",
  extra: "{}",
  host: "example.com",
  login: "",
  password: "",
  port: 443,
  schema: "",
} as ConnectionResponse;

const renderButton = () => render(<TestConnectionButton connection={connection} />, { wrapper: Wrapper });

describe("TestConnectionButton", () => {
  beforeEach(() => {
    testStatus = undefined;
    configValue = "Enabled";
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  it("enqueues a worker test on click", () => {
    renderButton();

    fireEvent.click(screen.getByRole("button"));

    expect(mutate).toHaveBeenCalledWith({
      requestBody: {
        conn_type: "http",
        connection_id: "my_conn",
        description: "",
        extra: "{}",
        host: "example.com",
        login: "",
        password: "",
        port: 443,
        schema: "",
      },
    });
  });

  it("shows a success toast when the test reaches a success state", () => {
    testStatus = { result_message: "It works", state: "success" };
    renderButton();

    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({ description: "It works", type: "success" }),
    );
  });

  it("shows an error toast when the test reaches a failed state", () => {
    testStatus = { result_message: "Connection refused", state: "failed" };
    renderButton();

    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({ description: "Connection refused", type: "error" }),
    );
  });

  it("does not toast while the test is still running", () => {
    testStatus = { state: "running" };
    renderButton();

    expect(create).not.toHaveBeenCalled();
  });

  it("shows the server error message when enqueuing fails", () => {
    renderButton();

    fireEvent.click(screen.getByRole("button"));
    enqueueOptions.onError({
      message: "An active connection test already exists for connection_id `my_conn`.",
      status: 409,
    });

    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({
        description: "An active connection test already exists for connection_id `my_conn`.",
        title: "connections.testError.title",
        type: "error",
      }),
    );
  });

  it("is disabled when the feature is turned off", () => {
    configValue = "Disabled";
    renderButton();

    expect(screen.getByRole("button")).toBeDisabled();
  });
});
