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

import { getAdditionalPageOffsets } from "./useTimeScheduleData";

describe("getAdditionalPageOffsets", () => {
  it("uses the API page size without skipping entries", () => {
    expect(
      getAdditionalPageOffsets({ pageSize: 100, requestedEntryCount: 2000, totalEntries: 2000 }),
    ).toEqual(Array.from({ length: 19 }, (_, index) => (index + 1) * 100));
    expect(getAdditionalPageOffsets({ pageSize: 100, requestedEntryCount: 600, totalEntries: 2000 })).toEqual(
      [100, 200, 300, 400, 500],
    );
    expect(getAdditionalPageOffsets({ pageSize: 100, requestedEntryCount: 1000, totalEntries: 250 })).toEqual(
      [100, 200],
    );
  });
});
