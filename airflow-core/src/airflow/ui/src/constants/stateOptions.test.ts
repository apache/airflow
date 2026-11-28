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

import { $DagRunType } from "openapi/requests/schemas.gen";

import { dagRunTypeOptions } from "./stateOptions";

describe("dagRunTypeOptions", () => {
  it("offers every run type the API accepts as a filter", () => {
    // The run_type filter sends these values straight to the API, which validates them against
    // DagRunType. A run type missing here is one nobody can filter for, even though the list it
    // filters shows those runs. `Array<...>` is not exhaustive on its own, so tsc cannot see this.
    const offered = dagRunTypeOptions.items.map((option) => option.value);
    const missing = $DagRunType.enum.filter((runType) => !offered.includes(runType));

    expect(missing).toEqual([]);
  });
});
