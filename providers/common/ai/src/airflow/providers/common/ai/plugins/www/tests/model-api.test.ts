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

import { createModelApi } from "../src/model-api.ts";

afterEach(() => mock.restoreAll());

function recordXComReads(): URL[] {
  const calls: URL[] = [];
  mock.method(globalThis, "fetch", async (url: string) => {
    calls.push(new URL(url, "http://localhost"));
    return new Response(JSON.stringify({ value: {} }), { status: 200 });
  });
  return calls;
}

test("both model XCom reads address the loop pass", async () => {
  const calls = recordXComReads();
  const region = { region_id: "00000000-0000-0000-0000-000000000123", region_index: 2 };
  const api = createModelApi("dag", "run", "body.agent", -1, region);

  await api.fetchModelName();
  await api.fetchUsage();

  assert.deepEqual(
    calls.map((url) => url.pathname.split("/").at(-1)),
    ["__AIRFLOW__COMMON_AI_MODEL_NAME__", "usage"],
  );
  for (const url of calls) {
    assert.equal(url.searchParams.get("region_id"), region.region_id);
    assert.equal(url.searchParams.get("region_index"), "2");
    assert.equal(url.searchParams.get("map_index"), "-1");
  }
});

test("a task outside a loop keeps the map-only XCom read", async () => {
  const calls = recordXComReads();

  const api = createModelApi("dag", "run", "agent", 0);
  await api.fetchModelName();
  await api.fetchUsage();

  assert.equal(calls.length, 2);
  for (const url of calls) {
    assert.equal(url.searchParams.get("map_index"), "0");
    assert.equal(url.searchParams.has("region_id"), false);
    assert.equal(url.searchParams.has("region_index"), false);
  }
});
