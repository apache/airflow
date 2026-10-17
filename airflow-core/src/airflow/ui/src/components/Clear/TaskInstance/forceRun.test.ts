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

import { resolveClearOptions } from "./forceRun";

describe("resolveClearOptions", () => {
  it("passes the selected options through when force run is off", () => {
    expect(resolveClearOptions(["past", "future", "upstream", "downstream", "onlyFailed"], false)).toEqual({
      downstream: true,
      future: true,
      onlyFailed: true,
      past: true,
      upstream: true,
    });
  });

  it("forces upstream, downstream and onlyFailed off when force run is on, leaving past/future untouched", () => {
    expect(resolveClearOptions(["past", "future", "upstream", "downstream", "onlyFailed"], true)).toEqual({
      downstream: false,
      future: true,
      onlyFailed: false,
      past: true,
      upstream: false,
    });
  });

  it("has nothing selected and force run off resolve to all false", () => {
    expect(resolveClearOptions([], false)).toEqual({
      downstream: false,
      future: false,
      onlyFailed: false,
      past: false,
      upstream: false,
    });
  });
});
