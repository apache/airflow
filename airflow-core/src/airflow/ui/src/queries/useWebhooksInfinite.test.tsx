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
import type { PropsWithChildren } from "react";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { http, HttpResponse } from "msw";
import { expect, it } from "vitest";

import type { WebhookCollectionResponse } from "openapi/requests/types.gen";

import { server } from "src/mocks/server";

import { useWebhooksInfinite } from "./useWebhooksInfinite";

const getNames = (pages: Array<WebhookCollectionResponse> = []) =>
  pages.flatMap((page) => page.webhooks).map(({ name }) => name);

it("pages through the webhooks until it has them all", async () => {
  const names = ["alpha", "beta", "gamma"];
  const offsets: Array<string | null> = [];

  server.use(
    http.get("/api/v2/webhooks", ({ request }) => {
      const { searchParams } = new URL(request.url);
      const offset = Number(searchParams.get("offset"));

      offsets.push(searchParams.get("offset"));

      return HttpResponse.json({
        total_entries: names.length,
        webhooks: names.slice(offset, offset + Number(searchParams.get("limit"))).map((name) => ({
          connection_status: "ok",
          last_outcome: null,
          last_received_at: null,
          listed: true,
          name,
          source_type: "hmac_json",
          source_type_status: "ok",
        })),
      });
    }),
  );
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: PropsWithChildren) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  );

  const { result } = renderHook(() => useWebhooksInfinite({ limit: 2 }), { wrapper });

  await waitFor(() => expect(result.current.hasNextPage).toBe(true));
  await act(async () => {
    await result.current.fetchNextPage();
  });

  await waitFor(() => expect(getNames(result.current.data?.pages)).toEqual(names));
  expect(result.current.hasNextPage).toBe(false);
  expect(offsets).toEqual(["0", "2"]);
});
