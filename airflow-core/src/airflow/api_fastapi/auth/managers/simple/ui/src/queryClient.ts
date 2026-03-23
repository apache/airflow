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
import { client } from "openapi/requests/services.gen";

// Dynamically set the base URL for XHR requests based on the meta tag.
OpenAPI.BASE = document.querySelector("head>base")?.getAttribute("href") ?? "";
if (OpenAPI.BASE.endsWith("/")) {
  OpenAPI.BASE = OpenAPI.BASE.slice(0, -1);
}

// Configure the generated API client so requests include the subpath prefix
// when Airflow runs behind a reverse proxy (e.g. /team-a/auth/token instead of /auth/token).
client.setConfig({ baseURL: OpenAPI.BASE });

export const queryClient = new QueryClient({
  defaultOptions: {
    mutations: {
      retry: 1,
      retryDelay: 500,
    },
  },
});
