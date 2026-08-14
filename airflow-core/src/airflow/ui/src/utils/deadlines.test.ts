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
import type { TFunction } from "i18next";
import { describe, expect, it } from "vitest";

import type { DeadlineAlertResponse } from "openapi/requests/types.gen";

import { translateCompletionRule } from "./deadlines";

const translate = ((key: string, options?: { interval?: string; reference?: string }) =>
  options?.reference === undefined
    ? key
    : `${key}:${options.interval ?? ""}:${options.reference}`) as unknown as TFunction;

const baseAlert: DeadlineAlertResponse = {
  created_at: "2025-01-01T00:00:00Z",
  id: "alert-1",
  interval: null,
  name: null,
  reference_type: "DagRunLogicalDateDeadline",
};

const REFERENCE = "deadlineAlerts.referenceType.DagRunLogicalDateDeadline";

describe("translateCompletionRule", () => {
  it.each([
    [3600, `deadlineAlerts.completionRule:an hour:${REFERENCE}`],
    [null, `deadlineAlerts.completionRuleDynamic::${REFERENCE}`],
  ])("names the rule for an interval of %s seconds", (interval, expected) => {
    expect(translateCompletionRule(translate, { ...baseAlert, interval })).toBe(expected);
  });

  it("has no rule to name without an alert", () => {
    expect(translateCompletionRule(translate, undefined)).toBeUndefined();
  });
});
