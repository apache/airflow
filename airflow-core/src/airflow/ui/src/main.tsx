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
import * as ChakraUI from "@chakra-ui/react";
import * as EmotionReact from "@emotion/react";
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
import { ChakraCustomProvider } from "src/context/ChakraCustomProvider";
import { ColorModeProvider } from "src/context/colorMode";
import { TimezoneProvider } from "src/context/timezone";
import { router } from "src/router";
import { getRedirectPath } from "src/utils/links.ts";

import i18n from "./i18n/config";
import { client } from "./queryClient";

// Set React, ReactDOM, Chakra UI, and Emotion on globalThis so dynamically imported React
// plugins (e.g. HITL Review) use the host's copies instead of bundling their own.
Reflect.set(globalThis, "React", React);
Reflect.set(globalThis, "ReactDOM", ReactDOM);
Reflect.set(globalThis, "ReactJSXRuntime", ReactJSXRuntime);
Reflect.set(globalThis, "ReactRouterDOM", ReactRouterDOM);
Reflect.set(globalThis, "ChakraUI", ChakraUI);
Reflect.set(globalThis, "EmotionReact", EmotionReact);

// URLs that returned 403 Forbidden. Permissions won't change mid-session,
// so we block further requests to avoid spamming the server with polling.
const forbidden403Urls = new Set<string>();

// Block outgoing requests to URLs that previously returned 403.
// The request is aborted immediately so no network traffic occurs.
axios.interceptors.request.use((config) => {
  if (config.url !== undefined && forbidden403Urls.has(config.url)) {
    const controller = new AbortController();

    controller.abort();
    config.signal = controller.signal;
  }

  return config;
});

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

    // Track permission-based 403 URLs so future polling requests are blocked at the request interceptor.
    // Only block "Forbidden" (missing permissions), not other auth-related 403s.
    if (
      error.response?.status === 403 &&
      error.response.data.detail === "Forbidden" &&
      error.config?.url !== undefined
    ) {
      forbidden403Urls.add(error.config.url);
    }

    return Promise.reject(error);
  },
);

createRoot(document.querySelector("#root") as HTMLDivElement).render(
  <StrictMode>
    <I18nextProvider i18n={i18n}>
      <QueryClientProvider client={client}>
        <ChakraCustomProvider>
          <ColorModeProvider>
            <TimezoneProvider>
              <RouterProvider router={router} />
            </TimezoneProvider>
          </ColorModeProvider>
        </ChakraCustomProvider>
      </QueryClientProvider>
    </I18nextProvider>
  </StrictMode>,
);
