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

import React from "react";
import {Alert, Container, Heading, Text} from "@chakra-ui/react";

import {useCreateToken} from "src/queries/useCreateToken";
import {LoginForm} from "src/login/LoginForm";
import type {ApiError} from "openapi-gen/requests/core/ApiError";
import type {LoginResponse, HTTPExceptionResponse, HTTPValidationError} from "openapi-gen/requests/types.gen";

export type LoginBody = {
    username: string; password: string;
};

type ExpandedApiError = {
    body: HTTPExceptionResponse | HTTPValidationError;
} & ApiError;

export const Login = () => {
    const onSuccess = (data: LoginResponse) => {
        // Redirect to index page with the token
        globalThis.location.replace(`/?token=${data.jwt_token}`);
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
        <Alert.Root status="warning">
          <Alert.Indicator />
          <Alert.Content>
            <Alert.Title>Development-only auth manager configured</Alert.Title>
            <Alert.Description>
              The auth manager configured in your environment is the <strong>Simple Auth Manager</strong>, which is
              intended for development use only. It is not suitable for production and <strong>should not be used in
              a production environment</strong>.
            </Alert.Description>
          </Alert.Content>
        </Alert.Root>
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
