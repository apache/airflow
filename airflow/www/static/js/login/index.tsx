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

/* global document */

import React from "react";
import { createRoot } from "react-dom/client";
import createCache from "@emotion/cache";
import { Alert, AlertIcon, Container, Heading, Text } from "@chakra-ui/react";

import App from "src/App";
import LoginForm from "src/login/Form";
import { useSearchParams } from "react-router-dom";

// create shadowRoot
const root = document.querySelector("#root");
const shadowRoot = root?.attachShadow({ mode: "open" });
const cache = createCache({
  container: shadowRoot,
  key: "c",
});
const mainElement = document.getElementById("react-container");

const Login = () => {
  const [searchParams] = useSearchParams();
  const error = searchParams.get("error");

  return (
    <Container maxW="2xl" p="4" border="1px" borderColor="gray.500">
      <Heading mb={6} fontWeight="normal" size="lg">
        Sign in
      </Heading>

      {error && (
        <Alert status="warning" mb="2">
          <AlertIcon />
          Invalid credentials, please try again.
        </Alert>
      )}

      <Text mb={4}>Enter your login and password below:</Text>
      <LoginForm />
    </Container>
  );
};

export default Login;

if (mainElement) {
  shadowRoot?.appendChild(mainElement);
  const reactRoot = createRoot(mainElement);
  reactRoot.render(
    <App cache={cache}>
      <Login />
    </App>
  );
}
