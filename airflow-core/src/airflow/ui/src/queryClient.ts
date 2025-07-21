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
import { QueryClient } from "@tanstack/react-query";

import { OpenAPI } from "openapi/requests/core/OpenAPI";

// Dynamically set the base URL for XHR requests based on the meta tag.
OpenAPI.BASE = document.querySelector("head>base")?.getAttribute("href") ?? "";
if (OpenAPI.BASE.endsWith("/")) {
  OpenAPI.BASE = OpenAPI.BASE.slice(0, -1);
}

export const client = new QueryClient({
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
