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

import { commonNamePrefix } from "./commonNamePrefix";

const PREFIX = "mysql___d___dashboard___commerce___vbp___";

describe("commonNamePrefix", () => {
  it("finds the prefix most names share, ignoring a few differently named tasks", () => {
    const names = ["init", "skip_if_old_date"];

    for (const period of ["d", "m", "w"]) {
      for (const index of [0, 1, 2, 3, 4]) {
        names.push(`${PREFIX}${period}___${index}`);
      }
    }

    expect(commonNamePrefix(names)).toBe(PREFIX);
  });

  it("ends the prefix on a separator rather than mid-word", () => {
    expect(commonNamePrefix(["load_orders", "load_order_items", "load_users"])).toBe("load_");
  });

  it("returns an empty prefix when fewer than half of the names share one", () => {
    expect(commonNamePrefix(["extract_a", "extract_b", "transform", "load", "notify"])).toBe("");
  });

  it("never hides a whole name", () => {
    expect(commonNamePrefix(["etl_", "etl_"])).toBe("");
    expect(commonNamePrefix(["single_task"])).toBe("");
  });
});
