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

import React, {useState} from "react";
import {Alert, CloseButton, Container, Heading, Span, Text} from "@chakra-ui/react";

import {useCreateToken} from "src/queries/useCreateToken";
import {LoginForm} from "src/login/LoginForm";
import type {ApiError} from "openapi-gen/requests/core/ApiError";
import type {LoginResponse, HTTPExceptionResponse, HTTPValidationError} from "openapi-gen/requests/types.gen";
import { useSearchParams } from "react-router-dom";
import { useCookies } from 'react-cookie';

export type LoginBody = {
  username: string; password: string;
};

type ExpandedApiError = {
  body: HTTPExceptionResponse | HTTPValidationError;
} & ApiError;

const LOCAL_STORAGE_DISABLE_BANNER_KEY = "disable-sam-banner"

export const Login = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const [cookies, setCookie] = useCookies(['_token']);
  const [isBannerDisabled, setIsBannerDisabled] = useState(localStorage.getItem(LOCAL_STORAGE_DISABLE_BANNER_KEY))

  const onSuccess = (data: LoginResponse) => {
    // Redirect to appropriate page with the token
    const next = searchParams.get("next")

    setCookie('_token', data.jwt_token, {path: "/", secure: globalThis.location.protocol !== "http:"})

    globalThis.location.replace(`${next ?? ""}`);
  }
  const {createToken, error: err, isPending, setError} = useCreateToken({onSuccess});
  const error = err as ExpandedApiError;
  const error_message = error?.body?.detail;

  const onLogin = (data: LoginBody) => {
    setError(undefined)
    createToken(data)
  }

  return (
    <>
      {!isBannerDisabled &&
        <Alert.Root status="info">
          <Alert.Indicator />
          <Alert.Content>
            <Alert.Title>Simple auth manager enabled</Alert.Title>
            <Alert.Description>
              The Simple auth manager is intended for development and testing. If you're using it in production,
              ensure that access is controlled through other means.
              Please read <Span textDecoration={"underline"}><a href="https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/auth-manager/simple/index.html" target="_blank">the documentation</a></Span> to learn more about simple auth manager.
            </Alert.Description>
          </Alert.Content>
          <CloseButton
            pos="relative"
            top="-2"
            insetEnd="-2"
            onClick={() => {
              localStorage.setItem(LOCAL_STORAGE_DISABLE_BANNER_KEY, "1")
              setIsBannerDisabled(1)
            }}
          />
        </Alert.Root>

      }

      <Container mt={2} maxW="2xl" p="4" border="1px" borderColor="gray.500" borderWidth="1px" borderStyle="solid">
        <Heading mb={6} fontWeight="normal" size="lg" colorPalette="blue">
          Sign in
        </Heading>

        {error_message && (
          <Alert.Root status="warning" mb="2">{error_message}</Alert.Root>)
        }

        <Text mb={4}>Enter your login and password below:</Text>
        <LoginForm onLogin={onLogin} isPending={isPending} />
      </Container>
    </>);
};
