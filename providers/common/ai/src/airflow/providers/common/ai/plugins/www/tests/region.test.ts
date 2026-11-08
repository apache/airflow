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
import { test } from "node:test";

import { isSelectionPending, regionOf, regionQuery } from "../src/region.ts";

const regionId = "00000000-0000-0000-0000-000000000123";

for (const regionIndex of [-1, 0, 2]) {
  test(`a loop task instance at region index ${regionIndex} addresses its pass`, () => {
    const region = regionOf({ id: "ti", region_id: regionId, region_index: regionIndex });

    assert.deepEqual(region, { region_id: regionId, region_index: regionIndex });
    assert.equal(regionQuery(region), `&region_id=${regionId}&region_index=${regionIndex}`);
  });
}

test("a task instance outside a loop addresses no region", () => {
  assert.equal(regionOf({ id: "ti" }), undefined);
  assert.equal(regionQuery(regionOf({ id: "ti" })), "");
});

test("only a host that passes an unresolved task instance is pending", () => {
  assert.equal(isSelectionPending({ taskInstance: undefined }), true);
  assert.equal(isSelectionPending({ taskInstance: { id: "ti" } }), false);
  assert.equal(isSelectionPending({}), false);
});
