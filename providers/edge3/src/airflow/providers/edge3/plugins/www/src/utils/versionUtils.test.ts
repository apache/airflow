
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

import { getLegacyRouterNavigation } from "src/utils/versionUtils";
import { describe, expect, it } from "vitest";

describe("getLegacyRouterNavigation", () => {
  it("returns true for versions <= 3.1.6", () => {
    expect(getLegacyRouterNavigation("3.1.6")).toBe(true);
    expect(getLegacyRouterNavigation("3.1.5")).toBe(true);
    expect(getLegacyRouterNavigation("3.0.0")).toBe(true);
  });

  it("returns false for versions > 3.1.6", () => {
    expect(getLegacyRouterNavigation("3.1.7")).toBe(false);
    expect(getLegacyRouterNavigation("3.1.7rc1")).toBe(false);
    expect(getLegacyRouterNavigation("3.2.0")).toBe(false);
    expect(getLegacyRouterNavigation("4.0.0")).toBe(false);
  });

  it("returns undefined for invalid versions", () => {
    expect(getLegacyRouterNavigation("invalid")).toBe(undefined);
    expect(getLegacyRouterNavigation("")).toBe(undefined);
  });
});
