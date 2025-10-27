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
import { describe, it, expect } from "vitest";

import { getDuration, renderDuration } from "./datetimeUtils";

describe("getDuration", () => {
  it("handles durations less than 60 seconds", () => {
    const start = "2024-03-14T10:00:00.000Z";
    const end = "2024-03-14T10:00:05.5111111Z";

    expect(getDuration(start, end)).toBe("00:00:05.511");
  });

  it("handles durations spanning multiple days", () => {
    const start = "2024-03-14T10:00:00.000Z";
    const end = "2024-03-17T15:30:45.000Z";

    expect(getDuration(start, end)).toBe("3d05:30:45");
  });

  it("handles exactly 24 hours", () => {
    const start = "2024-03-14T10:00:00.000Z";
    const end = "2024-03-15T10:00:00.000Z";

    expect(getDuration(start, end)).toBe("1d00:00:00");
  });

  it("handles hours and minutes without days", () => {
    const start = "2024-03-14T10:00:00.000Z";
    const end = "2024-03-14T12:30:00.000Z";

    expect(getDuration(start, end)).toBe("02:30:00");
  });

  it("handles small, null or undefined values", () => {
    // eslint-disable-next-line unicorn/no-null
    expect(getDuration(null, null)).toBe(undefined);
    expect(getDuration(undefined, undefined)).toBe(undefined);
    expect(renderDuration(0.000_01)).toBe(undefined);
  });
});
