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

import assert from "node:assert/strict";
import { afterEach, mock, test } from "node:test";

import { createApi } from "../src/api.ts";

afterEach(() => mock.restoreAll());

for (const regionIndex of [-1, 0, 2]) {
  test(`all review requests preserve region index ${regionIndex}`, async () => {
    const calls: URL[] = [];
    mock.method(globalThis, "fetch", async (url: string) => {
      calls.push(new URL(url, "http://localhost"));
      return new Response("{}", { status: 200 });
    });
    const region = { region_id: "00000000-0000-0000-0000-000000000123", region_index: regionIndex };
    const api = createApi("dag", "run", "body.review", -1, region);

    await api.fetchSession();
    await api.submitFeedback("revise");
    await api.approve();
    await api.reject();

    assert.equal(calls.length, 4);
    for (const url of calls) {
      assert.equal(url.searchParams.get("region_id"), region.region_id);
      assert.equal(url.searchParams.get("region_index"), String(regionIndex));
      assert.equal(url.searchParams.get("map_index"), "-1");
    }
  });
}

test("older hosts preserve the map-only request contract", async () => {
  let request: URL | undefined;
  mock.method(globalThis, "fetch", async (url: string) => {
    request = new URL(url, "http://localhost");
    return new Response("{}", { status: 200 });
  });

  await createApi("dag", "run", "review", 0).fetchSession();

  assert.equal(request?.searchParams.get("map_index"), "0");
  assert.equal(request?.searchParams.has("region_id"), false);
  assert.equal(request?.searchParams.has("region_index"), false);
});
