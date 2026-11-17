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

import {
  pausedStateFromSearchParams,
  pausedStateToQuery,
  pausedStateToSearchParams,
  type PausedStateValue,
} from "./pausedStateParams";

describe("pausedStateFromSearchParams", () => {
  it.each([
    { expected: "not_paused", url: "paused=false" },
    { expected: "paused", url: "paused=true" },
    { expected: "active", url: "scheduling_state=active" },
    { expected: "draining", url: "scheduling_state=draining" },
    // Links written against the short-lived Scheduling state filter.
    { expected: "paused", url: "scheduling_state=paused" },
    { expected: undefined, url: "" },
  ])("reads $url as $expected", ({ expected, url }) => {
    expect(pausedStateFromSearchParams(new URLSearchParams(url))).toBe(expected);
  });

  it("prefers the narrower scheduling state over paused=false", () => {
    // Draining Dags are not paused yet, so paused=false alone would also show them.
    expect(pausedStateFromSearchParams(new URLSearchParams("paused=false&scheduling_state=draining"))).toBe(
      "draining",
    );
  });
});

describe("pausedStateToSearchParams", () => {
  it.each<PausedStateValue>(["active", "draining", "not_paused", "paused"])(
    "writes %s so that reading the URL back gives the same choice",
    (value) => {
      const params = new URLSearchParams();

      for (const [key, param] of Object.entries(pausedStateToSearchParams(value))) {
        if (param !== undefined) {
          params.set(key, param);
        }
      }

      expect(pausedStateFromSearchParams(params)).toBe(value);
    },
  );
});

describe("pausedStateToQuery", () => {
  it.each([
    { expected: { paused: false, schedulingState: undefined }, value: "not_paused" },
    { expected: { paused: true, schedulingState: undefined }, value: "paused" },
    { expected: { paused: undefined, schedulingState: "active" }, value: "active" },
    { expected: { paused: undefined, schedulingState: "draining" }, value: "draining" },
    { expected: { paused: undefined, schedulingState: undefined }, value: undefined },
  ] as const)("asks the API for $value as $expected", ({ expected, value }) => {
    expect(pausedStateToQuery(value)).toEqual(expected);
  });
});
