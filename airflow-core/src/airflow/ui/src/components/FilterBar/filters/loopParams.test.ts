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

import { decodeLoopOption, encodeLoopOption } from "src/queries/useLoopFilterOptions";

import { loopFromSearchParams, loopToSearchParams } from "./loopParams";

describe("loopFromSearchParams", () => {
  it("drops an iteration that has no loop to count it", () => {
    expect(loopFromSearchParams(new URLSearchParams("iteration=2"))).toBeUndefined();
  });

  it("reads a loop on its own as every iteration", () => {
    expect(loopFromSearchParams(new URLSearchParams("loop_id=batch"))).toEqual({
      iteration: undefined,
      loopId: "batch",
    });
  });

  it("reads a loop and iteration together", () => {
    expect(loopFromSearchParams(new URLSearchParams("loop_id=batch&iteration=2"))).toEqual({
      iteration: "2",
      loopId: "batch",
    });
  });
});

describe("loopToSearchParams", () => {
  it("clears both params when the pill is removed", () => {
    expect(loopToSearchParams(undefined)).toEqual({ iteration: undefined, loop_id: undefined });
  });

  it("clears a stale iteration when the whole loop is chosen", () => {
    expect(loopToSearchParams({ loopId: "batch" })).toEqual({ iteration: undefined, loop_id: "batch" });
  });

  it("writes both when one pass is chosen", () => {
    expect(loopToSearchParams({ iteration: "2", loopId: "batch" })).toEqual({
      iteration: "2",
      loop_id: "batch",
    });
  });
});

describe("loop option encoding", () => {
  it("round-trips a dotted loop id that contains the separator's characters", () => {
    expect(decodeLoopOption(encodeLoopOption("outer.inner", 3))).toEqual({
      iteration: "3",
      loopId: "outer.inner",
    });
  });

  it("round-trips a whole loop", () => {
    expect(decodeLoopOption(encodeLoopOption("outer.inner"))).toEqual({ loopId: "outer.inner" });
  });
});
