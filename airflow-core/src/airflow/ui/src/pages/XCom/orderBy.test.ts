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

import { sortingToOrderBy } from "./orderBy";

describe("sortingToOrderBy", () => {
  it("keeps every XCom sort entry in order", () => {
    expect(
      sortingToOrderBy([
        { desc: false, id: "key" },
        { desc: true, id: "timestamp" },
      ]),
    ).toEqual(["key", "-timestamp"]);
  });

  it("maps the task display column to the API field", () => {
    expect(sortingToOrderBy([{ desc: false, id: "task_display_name" }])).toEqual(["task_id"]);
  });

  it("returns undefined without sorting", () => {
    expect(sortingToOrderBy([])).toBeUndefined();
  });
});
