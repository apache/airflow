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
import { afterEach, describe, expect, it } from "vitest";

import { pruneLegacyDependencyKeys, SHOW_ALL_DEPENDENCIES_KEY } from "./localStorage";

describe("pruneLegacyDependencyKeys", () => {
  afterEach(() => {
    globalThis.localStorage.clear();
  });

  it("removes only the legacy per-Dag dependency keys, leaving everything else intact", () => {
    globalThis.localStorage.setItem("dependencies-my_dag", "all");
    globalThis.localStorage.setItem("dependencies-other_dag", "tasks");
    globalThis.localStorage.setItem(SHOW_ALL_DEPENDENCIES_KEY, "true");
    globalThis.localStorage.setItem("direction-my_dag", "RIGHT");

    pruneLegacyDependencyKeys();

    expect(globalThis.localStorage.getItem("dependencies-my_dag")).toBeNull();
    expect(globalThis.localStorage.getItem("dependencies-other_dag")).toBeNull();
    // The global toggle and unrelated Dag-scoped keys are untouched.
    expect(globalThis.localStorage.getItem(SHOW_ALL_DEPENDENCIES_KEY)).toBe("true");
    expect(globalThis.localStorage.getItem("direction-my_dag")).toBe("RIGHT");
  });

  it("is a no-op when there are no legacy keys", () => {
    globalThis.localStorage.setItem(SHOW_ALL_DEPENDENCIES_KEY, "false");

    pruneLegacyDependencyKeys();

    expect(globalThis.localStorage.getItem(SHOW_ALL_DEPENDENCIES_KEY)).toBe("false");
    expect(globalThis.localStorage.length).toBe(1);
  });
});
