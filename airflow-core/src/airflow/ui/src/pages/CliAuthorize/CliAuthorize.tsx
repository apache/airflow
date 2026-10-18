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

import { Box, Button, Code, Container, Heading, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import { useAuthLinksServiceAuthorizeCli, useAuthLinksServiceGetCurrentUserInfo } from "openapi/queries";
import type { CliAuthorizeResponse } from "openapi/requests/types.gen";

import { ErrorAlert } from "src/components/ErrorAlert";
import { Logo } from "src/components/Logo";

import { useDocumentTitle } from "src/utils";

// Same limits as the server: airflowctl listens on an ephemeral port, so a link naming anything else did not come from it.
const MIN_PORT = 1024;
const MAX_PORT = 65_535;
// Unpadded base64url of a SHA-256 digest (RFC 7636 S256).
const CODE_CHALLENGE = /^[\w-]{43}$/u;

export type CliLoginRequest = {
  codeChallenge: string;
  port: number;
  state: string;
};

export const parseCliLoginRequest = (params: URLSearchParams): CliLoginRequest | undefined => {
  const port = Number(params.get("port"));
  const state = params.get("state") ?? "";
  const codeChallenge = params.get("code_challenge") ?? "";
  const method = params.get("code_challenge_method") ?? "S256";

  const isValid =
    Number.isInteger(port) &&
    port >= MIN_PORT &&
    port <= MAX_PORT &&
    state !== "" &&
    CODE_CHALLENGE.test(codeChallenge) &&
    method === "S256";

  return isValid ? { codeChallenge, port, state } : undefined;
};

// airflowctl prints the same characters, so the user can tell this request is the one their terminal started.
export const getConfirmationCode = (codeChallenge: string): string => codeChallenge.slice(0, 8);

// A top-level navigation: Chrome's Local Network Access prompts on fetch() to loopback but not on this.
const buildCallbackUrl = (port: number, params: Record<string, string>): string =>
  `http://127.0.0.1:${String(port)}/callback?${new URLSearchParams(params).toString()}`;

const Notice = ({ children }: { readonly children: string }) => (
  <Box alignItems="center" display="flex" justifyContent="center" pt={36} px={4}>
    <Container maxW="lg">
      <VStack gap={6} textAlign="center">
        <Logo height="50px" width="50px" />
        <Text fontSize="lg">{children}</Text>
      </VStack>
    </Container>
  </Box>
);

export const CliAuthorize = () => {
  const { t: translate } = useTranslation();
  const [searchParams] = useSearchParams();
  const request = parseCliLoginRequest(searchParams);
  const [isRedirecting, setIsRedirecting] = useState(false);
  const { data: currentUser } = useAuthLinksServiceGetCurrentUserInfo();

  useDocumentTitle(translate("cliLogin.title"));

  const redirect = (url: string) => {
    setIsRedirecting(true);
    globalThis.location.replace(url);
  };

  const {
    error,
    isPending,
    mutate: authorize,
  } = useAuthLinksServiceAuthorizeCli({
    onSuccess: ({ code }: CliAuthorizeResponse) => {
      if (request) {
        redirect(buildCallbackUrl(request.port, { code, state: request.state }));
      }
    },
  });

  // The server already forbids framing this page by header; keeping the buttons away when framed
  // covers a browser that ignores the header, since a frame could hide a decoy over Approve.
  if (globalThis.self !== globalThis.top) {
    return <Notice>{translate("cliLogin.framed")}</Notice>;
  }

  if (!request) {
    return <Notice>{translate("cliLogin.invalidLink")}</Notice>;
  }

  if (isRedirecting) {
    return <Notice>{translate("cliLogin.redirecting")}</Notice>;
  }

  return (
    <Box alignItems="center" display="flex" justifyContent="center" pt={24} px={4}>
      <Container maxW="lg">
        <VStack gap={6} textAlign="center">
          <Logo height="50px" width="50px" />
          <Heading>{translate("cliLogin.title")}</Heading>
          <Text fontSize="lg">
            {translate("cliLogin.description", { username: currentUser?.username ?? "" })}
          </Text>
          <Text>{translate("cliLogin.onlyApprove")}</Text>
          <Code data-testid="confirmation-code" fontSize="2xl" px={4} py={2}>
            {getConfirmationCode(request.codeChallenge)}
          </Code>
          <ErrorAlert error={error} />
          <HStack gap={4}>
            <Button
              colorPalette="blue"
              disabled={currentUser === undefined}
              loading={isPending}
              onClick={() =>
                authorize({
                  requestBody: {
                    code_challenge: request.codeChallenge,
                    code_challenge_method: "S256",
                    port: request.port,
                  },
                })
              }
              size="lg"
            >
              {translate("cliLogin.approve")}
            </Button>
            <Button
              onClick={() =>
                redirect(buildCallbackUrl(request.port, { error: "access_denied", state: request.state }))
              }
              size="lg"
              variant="outline"
            >
              {translate("cliLogin.deny")}
            </Button>
          </HStack>
        </VStack>
      </Container>
    </Box>
  );
};
