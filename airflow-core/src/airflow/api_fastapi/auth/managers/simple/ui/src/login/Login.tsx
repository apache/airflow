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
import { Alert, CloseButton, Container, Heading, Span, Text, Box, HStack } from "@chakra-ui/react";
import type { LoginResponse } from "openapi-gen/requests/types.gen";
import { useState, useEffect } from "react";
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

  // Set page title for SEO
  useEffect(() => {
    document.title = "Sign In - Apache Airflow";

    // Add meta description if not present
    let metaDescription = document.querySelector('meta[name="description"]');

    if (!metaDescription) {
      metaDescription = document.createElement("meta");
      metaDescription.setAttribute("name", "description");
      document.head.append(metaDescription);
    }
    metaDescription.setAttribute(
      "content",
      "Sign in to your Apache Airflow account to access workflow management, data pipelines, and orchestration tools.",
    );
  }, []);

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
    <Box
      _dark={{
        bg: "gray.900",
      }}
      alignItems="center"
      aria-label="Login page"
      as="main"
      bg="gray.50"
      display="flex"
      justifyContent="center"
      minH="100vh"
      p={4}
      role="main"
    >
      <Container
        _dark={{
          bg: "gray.800",
        }}
        aria-labelledby="login-heading"
        as="section"
        bg="white"
        borderRadius="lg"
        boxShadow="lg"
        maxW="md"
        p={8}
      >
        {/* Header Section with improved semantic structure */}
        <header>
          <HStack gap={3} mb={6}>
            <AirflowPin aria-hidden="true" height="35px" role="img" width="35px" />
            <Heading
              _dark={{ color: "white" }}
              as="h1"
              color="gray.800"
              fontWeight="normal"
              id="login-heading"
              size="xl"
            >
              Sign into Airflow
            </Heading>
          </HStack>
        </header>

        {/* Error Alert with improved accessibility */}
        {Boolean(error) && (
          <Box aria-live="polite" mb={4} role="alert">
            <ErrorAlert error={error} />
          </Box>
        )}

        <Text _dark={{ color: "gray.300" }} color="gray.600" id="login-instructions" mb={4}>
          Enter your username and password below:
        </Text>

        <LoginForm aria-describedby="login-instructions" isPending={isPending} onLogin={onLogin} />

        {isBannerDisabled === null && (
          <Alert.Root aria-labelledby="banner-title" mt={5} role="banner" status="info">
            <Alert.Indicator aria-hidden="true" />
            <Alert.Content>
              <Alert.Title id="banner-title">Simple auth manager enabled</Alert.Title>
              <Alert.Description>
                The Simple auth manager is intended for development and testing. If you&apos;re using it in
                production, ensure that access is controlled through other means. Please read{" "}
                <Span textDecoration="underline">
                  <a
                    aria-label="Learn more about simple auth manager (opens in new tab)"
                    href="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html"
                    rel="noreferrer noopener"
                    target="_blank"
                  >
                    the documentation
                  </a>
                </Span>{" "}
                to learn more about simple auth manager.
              </Alert.Description>
            </Alert.Content>
            <CloseButton
              aria-label="Close banner"
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
    </Box>
  );
};
