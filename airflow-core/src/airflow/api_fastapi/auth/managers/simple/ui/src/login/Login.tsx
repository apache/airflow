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
      metaDescription = document.createElement('meta');
      metaDescription.setAttribute('name', 'description');
      document.head.appendChild(metaDescription);
    }
    metaDescription.setAttribute('content', 'Sign in to your Apache Airflow account to access workflow management, data pipelines, and orchestration tools.');
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
      as="main"
      role="main"
      aria-label="Login page"
      _dark={{
        bg: "gray.900"
      }}
      alignItems="center"
      bg="gray.50"
      display="flex"
      justifyContent="center"
      minH="100vh"
      p={4}
    >
      <Container
        as="section"
        aria-labelledby="login-heading"
        _dark={{
          bg: "gray.800"
        }}
        bg="white"
        borderRadius="lg"
        boxShadow="lg"
        maxW="md"
        p={8}
      >
        {/* Header Section with improved semantic structure */}
        <header>
          <HStack gap={3} mb={6}>
            <AirflowPin 
              height="35px" 
              width="35px" 
              aria-hidden="true"
              role="img"
            />
            <Heading 
              as="h1"
              id="login-heading"
              fontWeight="normal" 
              size="xl"
              color="gray.800"
              _dark={{ color: "white" }}
            >
              Sign into Airflow
            </Heading>
          </HStack>
        </header>

        {/* Error Alert with improved accessibility */}
        {Boolean(error) && (
          <Box mb={4} role="alert" aria-live="polite">
            <ErrorAlert error={error} />
          </Box>
        )}

        <Text 
          mb={4}
          color="gray.600"
          _dark={{ color: "gray.300" }}
          id="login-instructions"
        >
          Enter your username and password below:
        </Text>
        
        <LoginForm 
          isPending={isPending} 
          onLogin={onLogin}
          aria-describedby="login-instructions"
        />
        
        {isBannerDisabled === null && (
          <Alert.Root 
            mt={5} 
            status="info"
            role="banner"
            aria-labelledby="banner-title"
          >
            <Alert.Indicator aria-hidden="true" />
            <Alert.Content>
              <Alert.Title id="banner-title">Simple auth manager enabled</Alert.Title>
              <Alert.Description>
                The Simple auth manager is intended for development and testing. If you&apos;re using it in
                production, ensure that access is controlled through other means. Please read{" "}
                <Span textDecoration="underline">
                  <a
                    href="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html"
                    rel="noreferrer noopener"
                    target="_blank"
                    aria-label="Learn more about simple auth manager (opens in new tab)"
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
              aria-label="Close banner"
            />
          </Alert.Root>
        )}
      </Container>
    </Box>
  );
};
