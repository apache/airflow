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
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import type { ConnectionResponse } from "openapi/requests/types.gen";

import i18n from "src/i18n/config";
import { Wrapper } from "src/utils/Wrapper";

import adminLocale from "../../../public/i18n/locales/en/admin.json";
import DuplicateConnectionButton from "./DuplicateConnectionButton";

const { mockAddConnection } = vi.hoisted(() => ({ mockAddConnection: vi.fn() }));

vi.mock("src/queries/useAddConnection", () => ({
  useAddConnection: () => ({ addConnection: mockAddConnection, error: undefined, isPending: false }),
}));

vi.mock("src/queries/useConnectionTypeMeta", () => ({
  useConnectionTypeMeta: () => ({
    formattedData: {},
    hookNames: { postgres: "Postgres" },
    isPending: false,
    keysList: ["postgres"],
  }),
}));

vi.mock("src/queries/useConfig.tsx", () => ({
  useConfig: () => false,
}));

const baseConnection: ConnectionResponse = {
  conn_type: "postgres",
  connection_id: "prod_db",
  description: "Production",
  extra: null,
  host: "db.example.com",
  login: "admin",
  password: null,
  port: 5432,
  schema: "public",
  team_name: null,
};

const duplicate = async (connection: ConnectionResponse) => {
  render(<DuplicateConnectionButton connection={connection} disabled={false} />, { wrapper: Wrapper });
  fireEvent.click(screen.getByRole("button", { name: "Duplicate Connection" }));

  const connectionIdInput = await screen.findByRole("textbox", { name: /Connection ID/u });

  fireEvent.change(connectionIdInput, { target: { value: "staging_db" } });
  fireEvent.blur(connectionIdInput);
  const save = screen.getByRole("button", { name: /Save/u });

  await waitFor(() => expect(save).toBeEnabled());
  fireEvent.click(save);

  await waitFor(() => expect(mockAddConnection).toHaveBeenCalledOnce());

  return mockAddConnection.mock.calls[0]?.[0] as Record<string, string>;
};

describe("DuplicateConnectionButton", () => {
  beforeEach(() => {
    i18n.addResourceBundle("en", "admin", adminLocale, true, true);
    mockAddConnection.mockClear();
  });

  it("creates a connection under the new id with the copied values", async () => {
    const body = await duplicate(baseConnection);

    expect(body).toMatchObject({
      conn_type: "postgres",
      connection_id: "staging_db",
      description: "Production",
      host: "db.example.com",
      login: "admin",
      password: "",
      port: "5432",
      schema: "public",
    });
    expect(screen.queryByText(/are not copied/u)).not.toBeInTheDocument();
  });

  it("does not copy redacted values", async () => {
    const body = await duplicate({
      ...baseConnection,
      extra: JSON.stringify({ nested: { api_key: "***", keep: "x" }, region: "eu", token: "***" }),
      password: "***",
    });

    expect(body.password).toBe("");
    expect(JSON.parse(body.extra ?? "")).toEqual({ nested: { keep: "x" }, region: "eu" });
    expect(screen.getByText(/are not copied/u)).toBeInTheDocument();
  });
});
