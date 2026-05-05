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

/**
 * Variable API helpers — create and delete Airflow variables.
 */
import { expect } from "@playwright/test";

import { baseUrl, getRequestContext, type RequestLike } from "../shared";

/** Create a variable via the API. 409 is treated as success. */
export async function apiCreateVariable(
  source: RequestLike,
  options: { description?: string; key: string; value: string },
): Promise<void> {
  const { description, key, value } = options;
  const request = getRequestContext(source);

  await expect(async () => {
    const response = await request.post(`${baseUrl}/api/v2/variables`, {
      data: { description: description ?? "", key, value },
      headers: { "Content-Type": "application/json" },
      timeout: 10_000,
    });

    if (response.status() !== 409 && !response.ok()) {
      throw new Error(`Variable creation failed (${response.status()})`);
    }
  }).toPass({ intervals: [2000, 3000, 5000], timeout: 90_000 });
}

/** Delete a variable via the API. 404 is treated as success. */
export async function apiDeleteVariable(source: RequestLike, key: string): Promise<void> {
  const request = getRequestContext(source);

  const response = await request.delete(`${baseUrl}/api/v2/variables/${encodeURIComponent(key)}`, {
    timeout: 10_000,
  });

  // 404 = already deleted by another worker or cleanup; acceptable.
  if (response.status() === 404) {
    return;
  }

  if (!response.ok()) {
    const body = await response.text();

    throw new Error(`Variable deletion failed (${response.status()}): ${body}`);
  }
}
