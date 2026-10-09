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

import { loopsInScope } from "./useLoopFilterOptions";

const declared = ["batch", "outer.refine"];

describe("loopsInScope", () => {
  it("offers every loop where no group is in view", () => {
    expect(loopsInScope(declared)).toEqual(declared);
  });

  it("offers only the group's own loop on its page", () => {
    expect(loopsInScope(declared, "batch")).toEqual(["batch"]);
  });

  it("reports the enclosing loop for a group nested inside one", () => {
    expect(loopsInScope(declared, "outer.refine.body")).toEqual(["outer.refine"]);
  });

  it("offers nothing for a group that no loop contains", () => {
    expect(loopsInScope(declared, "unrelated")).toEqual([]);
  });

  it("does not treat a shared name prefix as containment", () => {
    expect(loopsInScope(["batch"], "batching")).toEqual([]);
  });
});
