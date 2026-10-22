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

import type { ParamsSpec } from "src/queries/useDagParams";

import { findDateBoundError } from "./findDateBoundError";

const afterStart = { formatExclusiveMinimum: { $data: "1/start" } };
const onOrAfterStart = { formatMinimum: { $data: "1/start" } };

type DateRange = {
  bound: Record<string, unknown>;
  end: string | null;
  format?: string;
  start: string | null;
};

const dateRange = ({ bound, end, format = "date", start }: DateRange) =>
  ({
    end: { description: null, schema: { format, ...bound }, value: end },
    start: { description: null, schema: { format, title: "Start" }, value: start },
  }) as unknown as ParamsSpec;

describe("findDateBoundError", () => {
  it.each([
    {
      bound: afterStart,
      end: "2026-10-01",
      messageKey: "flexibleForm.validationErrorAfter",
      start: "2026-10-01",
    },
    {
      bound: onOrAfterStart,
      end: "2026-10-01",
      messageKey: "flexibleForm.validationErrorOnOrAfter",
      start: "2026-10-02",
    },
  ])("names the referenced param when $messageKey fails", ({ bound, end, messageKey, start }) => {
    expect(findDateBoundError("end", dateRange({ bound, end, start }))).toEqual({
      bound: "Start",
      messageKey,
    });
  });

  it.each([
    { bound: onOrAfterStart, end: "2026-10-01", label: "an equal date is on or after", start: "2026-10-01" },
    {
      bound: afterStart,
      end: "2026-10-01T09:00:00Z",
      format: "date-time",
      label: "a date-time is later in UTC",
      start: "2026-10-01T10:00:00+02:00",
    },
    { bound: afterStart, end: null, label: "the end is empty", start: "2026-10-02" },
    { bound: afterStart, end: "2026-10-01", label: "the start is empty", start: null },
    {
      bound: { formatExclusiveMinimum: { $data: "1/missing" } },
      end: "2026-10-01",
      label: "the reference names no param",
      start: "2026-10-02",
    },
  ])("passes when $label", (range) => {
    expect(findDateBoundError("end", dateRange(range))).toBeUndefined();
  });
});
