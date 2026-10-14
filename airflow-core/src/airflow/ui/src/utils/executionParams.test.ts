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

import { setExecutionCoordinates, stripExecutionParams } from "./executionParams";

describe("stripExecutionParams", () => {
  it("drops the params that pin one execution or try and keeps the rest", () => {
    const source = new URLSearchParams("try_number=2&region_id=r&region_index=1&dag_run_state=failed");

    const stripped = stripExecutionParams(source);

    expect(stripped.toString()).toBe("dag_run_state=failed");
    expect(source.has("try_number")).toBe(true);
  });

  it("keeps the loop scope, which follows you between a loop's tasks", () => {
    const stripped = stripExecutionParams(new URLSearchParams("region_id=r&loop_id=batch&iteration=3"));

    expect(stripped.toString()).toBe("loop_id=batch&iteration=3");
  });
});

describe("setExecutionCoordinates", () => {
  it("drops a pass the new coordinate does not name", () => {
    const params = setExecutionCoordinates(new URLSearchParams("loop_id=batch&iteration=0"), "r", 4);

    // The row shown is now the newest pass, so keeping "0" would label it wrongly.
    expect(params.get("region_index")).toBe("4");
    expect(params.has("iteration")).toBe(false);
    expect(params.has("loop_id")).toBe(false);
  });

  it("leaves the pass alone when there is no coordinate to move to", () => {
    const params = setExecutionCoordinates(
      new URLSearchParams("loop_id=batch&iteration=0"),
      undefined,
      undefined,
    );

    expect(params.get("iteration")).toBe("0");
  });
});
