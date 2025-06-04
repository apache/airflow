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
import { Flex, Alert, CloseButton, Container, Heading, Span, Text } from "@chakra-ui/react";
import type { LoginResponse } from "openapi-gen/requests/types.gen";
import { useState } from "react";
import { useCookies } from "react-cookie";
import { useSearchParams } from "react-router-dom";

import { AirflowPin } from "src/AirflowPin";
import { ErrorAlert } from "src/alert/ErrorAlert";
import { LoginForm } from "src/login/LoginForm";
import { useCreateToken } from "src/queries/useCreateToken";

export type LoginBody = {
  password: string;
  username: string;
};

const isSafeUrl = (targetUrl: string): boolean => {
  try {
    const base = new URL(globalThis.location.origin);
    const target = new URL(targetUrl, base);

    return (target.protocol === "http:" || target.protocol === "https:") && target.origin === base.origin;
  } catch {
    return false;
  }
};

const LOCAL_STORAGE_DISABLE_BANNER_KEY = "disable-sam-banner";

export const Login = () => {
  const [searchParams] = useSearchParams();
  const [, setCookie] = useCookies(["_token"]);
  const [isBannerDisabled, setIsBannerDisabled] = useState(
    localStorage.getItem(LOCAL_STORAGE_DISABLE_BANNER_KEY),
  );

  const onSuccess = (data: LoginResponse) => {
    // Fallback similar to FabAuthManager, strip off the next
    const fallback = "/";

    // Redirect to appropriate page with the token
    const next = searchParams.get("next") ?? fallback;

    setCookie("_token", data.access_token, {
      path: "/",
      secure: globalThis.location.protocol !== "http:",
    });

    const redirectTarget = isSafeUrl(next) ? next : fallback;

    globalThis.location.replace(redirectTarget);
  };
  const { createToken, error, isPending, setError } = useCreateToken({
    onSuccess,
  });

  const onLogin = (data: LoginBody) => {
    setError(undefined);
    createToken(data);
  };

  return (
    <Container
      border="1px"
      borderColor="gray.emphasized"
      borderRadius={5}
      borderStyle="solid"
      borderWidth="1px"
      maxW="2xl"
      mt={2}
      p="4"
    >
      <Flex gap={2} mb={6}>
        <AirflowPin height="35px" width="35px" />
        <Heading colorPalette="blue" fontWeight="normal" size="xl">
          Sign into Airflow
        </Heading>
      </Flex>

      {Boolean(error) && <ErrorAlert error={error} />}

      <Text mb={4}>Enter your username and password below:</Text>
      <LoginForm isPending={isPending} onLogin={onLogin} />
      {isBannerDisabled === null && (
        <Alert.Root mt={5} status="info">
          <Alert.Indicator />
          <Alert.Content>
            <Alert.Title>Simple auth manager enabled</Alert.Title>
            <Alert.Description>
              The Simple auth manager is intended for development and testing. If you&apos;re using it in
              production, ensure that access is controlled through other means. Please read{" "}
              <Span textDecoration="underline">
                <a
                  href="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html"
                  rel="noreferrer"
                  target="_blank"
                >
                  the documentation
                </a>
              </Span>{" "}
              to learn more about simple auth manager.
            </Alert.Description>
          </Alert.Content>
          <CloseButton
            insetEnd="-2"
            onClick={() => {
              localStorage.setItem(LOCAL_STORAGE_DISABLE_BANNER_KEY, "1");
              setIsBannerDisabled("1");
            }}
            pos="relative"
            top="-2"
          />
        </Alert.Root>
      )}
    </Container>
  );
};
