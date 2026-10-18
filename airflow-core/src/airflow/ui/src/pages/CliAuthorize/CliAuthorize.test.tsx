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
import { fireEvent, render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type * as OpenapiQueries from "openapi/queries";

import { BaseWrapper } from "src/utils/Wrapper";

import { CliAuthorize } from "./CliAuthorize";

const authorize = vi.hoisted(() => vi.fn());

type CurrentUser = { data: { id: string; teams: null; username: string } | undefined };

const currentUser = vi.hoisted((): CurrentUser => ({
  data: { id: "jdoe", teams: null, username: "jdoe" },
}));

vi.mock("openapi/queries", async (importOriginal) => ({
  ...(await importOriginal<typeof OpenapiQueries>()),
  useAuthLinksServiceAuthorizeCli: ({ onSuccess }: { onSuccess: (data: { code: string }) => void }) => ({
    error: undefined,
    isPending: false,
    mutate: (variables: unknown) => {
      authorize(variables);
      onSuccess({ code: "one-time-code" });
    },
  }),
  useAuthLinksServiceGetCurrentUserInfo: () => currentUser,
}));

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: (key: string, options?: { username?: string }) =>
      options?.username === undefined ? key : `${key} ${options.username}`,
  }),
}));

// Test vector from RFC 7636, appendix B.
const CHALLENGE = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";
const VALID_LINK = `/cli/authorize?port=53682&state=abc123&code_challenge=${CHALLENGE}&code_challenge_method=S256`;

const renderAt = (link: string) =>
  render(
    <MemoryRouter initialEntries={[link]}>
      <CliAuthorize />
    </MemoryRouter>,
    { wrapper: BaseWrapper },
  );

describe("CliAuthorize", () => {
  const replace = vi.fn();

  beforeEach(() => {
    vi.spyOn(globalThis.location, "replace").mockImplementation(replace);
  });

  afterEach(() => {
    currentUser.data = { id: "jdoe", teams: null, username: "jdoe" };
  });

  it("shows who is being authorized and the confirmation code airflowctl printed", () => {
    renderAt(VALID_LINK);

    expect(screen.getByText("cliLogin.description jdoe")).toBeInTheDocument();
    expect(screen.getByTestId("confirmation-code")).toHaveTextContent("E9Melhoa");
    expect(screen.getByRole("button", { name: "cliLogin.approve" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "cliLogin.deny" })).toBeInTheDocument();
  });

  it("asks for a code on approval and hands it to airflowctl's loopback callback", () => {
    renderAt(VALID_LINK);

    fireEvent.click(screen.getByRole("button", { name: "cliLogin.approve" }));

    expect(authorize).toHaveBeenCalledWith({
      requestBody: { code_challenge: CHALLENGE, code_challenge_method: "S256", port: 53_682 },
    });
    expect(replace).toHaveBeenCalledWith("http://127.0.0.1:53682/callback?code=one-time-code&state=abc123");
    expect(screen.getByText("cliLogin.redirecting")).toBeInTheDocument();
  });

  it("tells airflowctl about a denial without asking for a code", () => {
    renderAt(VALID_LINK);

    fireEvent.click(screen.getByRole("button", { name: "cliLogin.deny" }));

    expect(authorize).not.toHaveBeenCalled();
    expect(replace).toHaveBeenCalledWith("http://127.0.0.1:53682/callback?error=access_denied&state=abc123");
  });

  it("keeps the approve button disabled until it knows who is signed in", () => {
    currentUser.data = undefined;

    renderAt(VALID_LINK);

    expect(screen.getByRole("button", { name: "cliLogin.approve" })).toBeDisabled();
  });

  it.each([
    ["a privileged port", VALID_LINK.replace("port=53682", "port=80")],
    ["a port that does not exist", VALID_LINK.replace("port=53682", "port=70000")],
    ["no state", VALID_LINK.replace("state=abc123", "state=")],
    ["a malformed challenge", VALID_LINK.replace(CHALLENGE, "short")],
    ["the plain challenge method", VALID_LINK.replace("S256", "plain")],
  ])("refuses a link with %s", (_description, link) => {
    renderAt(link);

    expect(screen.getByText("cliLogin.invalidLink")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "cliLogin.approve" })).not.toBeInTheDocument();
  });

  it("offers no buttons when shown inside a frame", () => {
    vi.spyOn(globalThis, "top", "get").mockReturnValue({} as Window);

    renderAt(VALID_LINK);

    expect(screen.getByText("cliLogin.framed")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "cliLogin.approve" })).not.toBeInTheDocument();
  });
});
