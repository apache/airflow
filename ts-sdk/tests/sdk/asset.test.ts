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

import {
  Asset,
  AssetNameRef,
  type AssetRefSpec,
  type AssetSpec,
  AssetUriRef,
} from "../../src/sdk/asset.js";

describe("Asset", () => {
  it.each([
    ["a name only", { name: "orders" }, { name: "orders", uri: "orders" }],
    [
      "a uri only",
      { uri: "s3://warehouse/orders" },
      { name: "s3://warehouse/orders", uri: "s3://warehouse/orders" },
    ],
    [
      "both",
      { name: "orders", uri: "s3://warehouse/orders" },
      { name: "orders", uri: "s3://warehouse/orders" },
    ],
  ])("defaults the missing identity field like Python when given %s", (_label, spec, identity) => {
    expect(new Asset(spec)).toMatchObject(identity);
  });

  it.each([
    ["no identity", {}, /Asset requires a name or a uri/],
    ["a non-string name", { name: 1 }, /Asset name must be a non-empty string/],
    ["an empty uri", { name: "a", uri: "" }, /Asset uri must be a non-empty string/],
  ])("rejects %s", (_label, spec, message) => {
    expect(() => new Asset(spec as unknown as AssetSpec)).toThrowError(message);
  });
});

describe("Asset.ref", () => {
  it.each([
    ["name", { name: "orders" }, AssetNameRef],
    ["uri", { uri: "s3://warehouse/orders" }, AssetUriRef],
  ])("returns a reference by %s", (_label, spec, refClass) => {
    const ref = Asset.ref(spec);
    expect(ref).toBeInstanceOf(refClass);
    expect(ref).toMatchObject(spec);
  });

  it.each([
    ["both", { name: "orders", uri: "s3://warehouse/orders" }, /either a name or a uri, not both/],
    ["neither", {}, /Asset\.ref\(\) requires a name or a uri/],
    ["an empty name", { name: "" }, /Asset name must be a non-empty string/],
  ])("rejects %s", (_label, spec, message) => {
    expect(() => Asset.ref(spec as AssetRefSpec)).toThrowError(message);
  });
});
