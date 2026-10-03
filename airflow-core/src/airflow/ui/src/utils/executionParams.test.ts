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

import { stripExecutionParams } from "./executionParams";

describe("stripExecutionParams", () => {
  it("drops the params that pin one execution, try or loop iteration and keeps the rest", () => {
    const source = new URLSearchParams(
      "try_number=2&region_id=r&region_index=1&iteration=3&loop_region_id=l&dag_run_state=failed",
    );

    const stripped = stripExecutionParams(source);

    expect(stripped.toString()).toBe("dag_run_state=failed");
    expect(source.has("try_number")).toBe(true);
  });
});
