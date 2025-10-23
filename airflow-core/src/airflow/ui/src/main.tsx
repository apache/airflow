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
import { ChakraProvider } from "@chakra-ui/react";
import { QueryClientProvider } from "@tanstack/react-query";
import axios, { type AxiosError } from "axios";
import { StrictMode } from "react";
import React from "react";
import * as ReactDOM from "react-dom";
import { createRoot } from "react-dom/client";
import { I18nextProvider } from "react-i18next";
import { RouterProvider } from "react-router-dom";
import * as ReactRouterDOM from "react-router-dom";
import * as ReactJSXRuntime from "react/jsx-runtime";

import type { HTTPExceptionResponse } from "openapi/requests/types.gen";
import { ColorModeProvider } from "src/context/colorMode";
import { TimezoneProvider } from "src/context/timezone";
import { router } from "src/router";
import { getRedirectPath } from "src/utils/links.ts";

import i18n from "./i18n/config";
import { client } from "./queryClient";
import { system } from "./theme";

// Set React, ReactDOM, and ReactJSXRuntime on globalThis to share them with the dynamically imported React plugins.
// Only one instance of React should be used.
// Reflect will avoid type checking.
Reflect.set(globalThis, "React", React);
Reflect.set(globalThis, "ReactDOM", ReactDOM);
Reflect.set(globalThis, "ReactJSXRuntime", ReactJSXRuntime);
Reflect.set(globalThis, "ReactRouterDOM", ReactRouterDOM);

// redirect to login page if the API responds with unauthorized or forbidden errors
axios.interceptors.response.use(
  (response) => response,
  (error: AxiosError<HTTPExceptionResponse>) => {
    if (
      error.response?.status === 401 ||
      (error.response?.status === 403 && error.response.data.detail === "Invalid JWT token")
    ) {
      const params = new URLSearchParams();

      params.set("next", globalThis.location.href);
      const loginPath = getRedirectPath("api/v2/auth/login");

      globalThis.location.replace(`${loginPath}?${params.toString()}`);
    }

    return Promise.reject(error);
  },
);

createRoot(document.querySelector("#root") as HTMLDivElement).render(
  <StrictMode>
    <I18nextProvider i18n={i18n}>
      <ChakraProvider value={system}>
        <ColorModeProvider>
          <QueryClientProvider client={client}>
            <TimezoneProvider>
              <RouterProvider router={router} />
            </TimezoneProvider>
          </QueryClientProvider>
        </ColorModeProvider>
      </ChakraProvider>
    </I18nextProvider>
  </StrictMode>,
);
