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
import { describe, expect, it } from "vitest";

import { applyPluginTemplate } from "./pluginTemplate";

describe("applyPluginTemplate", () => {
  it.each([
    { key: "dagId", token: "{DAG_ID}" },
    { key: "runId", token: "{RUN_ID}" },
    { key: "taskId", token: "{TASK_ID}" },
    { key: "mapIndex", token: "{MAP_INDEX}" },
    { key: "regionId", token: "{REGION_ID}" },
    { key: "regionIndex", token: "{REGION_INDEX}" },
    { key: "assetId", token: "{ASSET_ID}" },
    { key: "assetUri", token: "{ASSET_URI}" },
  ])("substitutes $token from $key", ({ key, token }) => {
    expect(applyPluginTemplate(`/x/${token}`, { [key]: "value" })).toBe("/x/value");
  });

  it("substitutes every occurrence of a token", () => {
    expect(applyPluginTemplate("/{DAG_ID}/runs/{DAG_ID}", { dagId: "etl" })).toBe("/etl/runs/etl");
  });

  it("leaves a token with no value in place rather than emptying it", () => {
    expect(applyPluginTemplate("/{DAG_ID}/{RUN_ID}", { dagId: "etl" })).toBe("/etl/{RUN_ID}");
  });

  it("percent-encodes values that would otherwise change the URL's shape", () => {
    expect(applyPluginTemplate("/dags/{DAG_ID}", { dagId: "team/etl?x=1" })).toBe("/dags/team%2Fetl%3Fx%3D1");
  });

  it("encodes an asset URI, which routinely contains a scheme and slashes", () => {
    expect(applyPluginTemplate("/a?uri={ASSET_URI}", { assetUri: "s3://bucket/key" })).toBe(
      "/a?uri=s3%3A%2F%2Fbucket%2Fkey",
    );
  });

  // Pins the one deliberate inconsistency carried over from the code this was extracted from.
  // A map index is always `-?\d+`, so encoding would never change a real value.
  it("leaves the region tokens in place on a task instance that has none", () => {
    // Most task instances are not loop iterations; a blank would point the link at the
    // wrong row rather than failing visibly.
    expect(applyPluginTemplate("/x/{REGION_ID}/{REGION_INDEX}", { dagId: "d" })).toBe(
      "/x/{REGION_ID}/{REGION_INDEX}",
    );
  });

  it("leaves a numeric token unchanged, since encoding one is a no-op", () => {
    expect(applyPluginTemplate("/ti/{MAP_INDEX}", { mapIndex: "-1" })).toBe("/ti/-1");
  });

  it("returns a template with no tokens untouched", () => {
    expect(applyPluginTemplate("/static/page", { dagId: "etl" })).toBe("/static/page");
  });
});
