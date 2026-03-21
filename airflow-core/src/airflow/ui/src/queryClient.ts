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
import { MutationCache, QueryClient } from "@tanstack/react-query";

import { OpenAPI } from "openapi/requests/core/OpenAPI";
import { toaster } from "src/components/ui";
import i18n from "src/i18n/config";
import { getErrorStatus } from "src/utils";

// Dynamically set the base URL for XHR requests based on the meta tag.
OpenAPI.BASE = document.querySelector("head>base")?.getAttribute("href") ?? "";
if (OpenAPI.BASE.endsWith("/")) {
  OpenAPI.BASE = OpenAPI.BASE.slice(0, -1);
}

const RETRY_COUNT = 3;

const retryFunction = (failureCount: number, error: unknown) => {
  const { status } = error as { status?: number };

  // Do not retry for client errors (4xx). 429 should be eventually retried though.
  if (status !== undefined && status >= 400 && status < 500 && status !== 429) {
    return false;
  }

  return failureCount < RETRY_COUNT;
};

// Track active 403 toast to prevent duplicates when multiple mutations fail
let active403ToastId: string | undefined;

// Error handler for 403 (Forbidden) responses on user-initiated actions
const handle403Error = (error: unknown) => {
  // Check for 403 (Forbidden) only to avoid interfering with 401 (Auth) logic
  const status = getErrorStatus(error);

  if (status === 403) {
    // Only show one 403 toast at a time to prevent toast spam
    // when multiple mutations fail simultaneously
    if (active403ToastId === undefined || !toaster.isActive(active403ToastId)) {
      active403ToastId = toaster.create({
        description: i18n.t("errors.forbidden.description"),
        title: i18n.t("errors.forbidden.title"),
        type: "error",
      });
    }
  }
  // For other errors, let them bubble up to individual mutation handlers
};

export const client = new QueryClient({
  defaultOptions: {
    mutations: {
      retry: retryFunction,
    },
    queries: {
      initialDataUpdatedAt: new Date().setMinutes(-6), // make sure initial data is already expired
      refetchOnMount: true, // Refetches stale queries, not "always"
      refetchOnWindowFocus: false,
      retry: retryFunction,
      staleTime: 5 * 60 * 1000, // 5 minutes
    },
  },
  mutationCache: new MutationCache({
    onError: handle403Error,
  }),
});
