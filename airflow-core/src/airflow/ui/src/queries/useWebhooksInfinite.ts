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
import { useInfiniteQuery } from "@tanstack/react-query";

import { UseWebhookServiceGetWebhooksKeyFn } from "openapi/queries";
import { WebhookService } from "openapi/requests/services.gen";
import type { WebhookCollectionResponse } from "openapi/requests/types.gen";

export const useWebhooksInfinite = ({ limit }: { limit?: number } = {}) =>
  useInfiniteQuery({
    getNextPageParam: (lastPage: WebhookCollectionResponse, _allPages, lastPageParam: number) => {
      const nextOffset = lastPageParam + lastPage.webhooks.length;

      return nextOffset < lastPage.total_entries ? nextOffset : undefined;
    },
    initialPageParam: 0,
    queryFn: ({ pageParam }: { pageParam: number }) =>
      WebhookService.getWebhooks({ limit, offset: pageParam }),
    // Apart from the Webhooks tab's pages of the same route, which cache single pages.
    queryKey: UseWebhookServiceGetWebhooksKeyFn({ limit }, ["infinite"]),
  });
