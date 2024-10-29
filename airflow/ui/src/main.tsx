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
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import axios, { type AxiosError } from "axios";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router-dom";

import { TimezoneProvider } from "./context/timezone";
import { router } from "./router";
import theme from "./theme";

const queryClient = new QueryClient({
  defaultOptions: {
    mutations: {
      retry: 1,
      retryDelay: 500,
    },
    queries: {
      initialDataUpdatedAt: new Date().setMinutes(-6), // make sure initial data is already expired
      refetchOnMount: true, // Refetches stale queries, not "always"
      refetchOnWindowFocus: false,
      retry: 1,
      retryDelay: 500,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
});

// redirect to login page if the API responds with unauthorized or forbidden errors
axios.interceptors.response.use(
  (response) => response,
  (error: AxiosError) => {
    if (error.response?.status === 403 || error.response?.status === 401) {
      const params = new URLSearchParams();

      params.set("next", globalThis.location.href);
      globalThis.location.replace(`/login?${params.toString()}`);
    }

    return Promise.reject(error);
  },
);

const root = createRoot(document.querySelector("#root") as HTMLDivElement);

root.render(
  <ChakraProvider theme={theme}>
    <QueryClientProvider client={queryClient}>
      <TimezoneProvider>
        <RouterProvider router={router} />
      </TimezoneProvider>
    </QueryClientProvider>
  </ChakraProvider>,
);
