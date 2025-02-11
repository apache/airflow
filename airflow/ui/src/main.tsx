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
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router-dom";

import { ColorModeProvider } from "src/context/colorMode";
import { TimezoneProvider } from "src/context/timezone";
import { router } from "src/router";

import { TOKEN_STORAGE_KEY } from "./layouts/BaseLayout";
import { queryClient } from "./queryClient";
import { system } from "./theme";

// redirect to login page if the API responds with unauthorized or forbidden errors
axios.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 401) {
      const params = new URLSearchParams();

      params.set("next", globalThis.location.href);
      globalThis.location.replace(`/public/login?${params.toString()}`);
    }

    return Promise.reject(error);
  },
);

axios.interceptors.request.use((config) => {
  const token: string | null = localStorage.getItem(TOKEN_STORAGE_KEY);

  if (token !== null) {
    // usehooks-ts stores a JSON.stringified version of values, we cannot use usehooks-ts here because we are outside of
    // a react component. Therefore using bare localStorage.getItem and manually parsing the value.
    config.headers.Authorization = `Bearer ${JSON.parse(token)}`;
  }

  return config;
});

createRoot(document.querySelector("#root") as HTMLDivElement).render(
  <StrictMode>
    <ChakraProvider value={system}>
      <ColorModeProvider>
        <QueryClientProvider client={queryClient}>
          <TimezoneProvider>
            <RouterProvider router={router} />
          </TimezoneProvider>
        </QueryClientProvider>
      </ColorModeProvider>
    </ChakraProvider>
  </StrictMode>,
);
