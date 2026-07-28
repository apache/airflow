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
import { describe, it, expect, vi } from "vitest";

import type { HITLDetail } from "openapi/requests/types.gen";

import { getHITLParamsDict, getHITLState, isHITLPending } from "./hitl";

const mockTranslate = vi.fn((key: string) => key) as unknown as TFunction;

const createMockHITLDetail = (overrides?: Partial<HITLDetail>): HITLDetail =>
  ({
    assigned_users: [],
    body: "Test Body",
    chosen_options: [],
    created_at: new Date().toISOString(),
    defaults: ["Option1"],
    multiple: false,
    options: ["Option1", "Option2"],
    params: {},
    params_input: {},
    response_received: false,
    subject: "Test Subject",
    task_instance: {
      dag_id: "test_dag",
      dag_run_id: "test_run",
      map_index: -1,
      state: "deferred",
      task_id: "test_task",
    },
    ...overrides,
  }) as HITLDetail;

describe("isHITLPending", () => {
  it("treats a deferred task as pending", () => {
    expect(isHITLPending("deferred")).toBe(true);
  });

  it("treats an awaiting_input task as pending", () => {
    expect(isHITLPending("awaiting_input")).toBe(true);
  });

  it("treats a finished/cleared task as not pending", () => {
    expect(isHITLPending("success")).toBe(false);
    expect(isHITLPending(undefined)).toBe(false);
    expect(isHITLPending(null)).toBe(false);
  });
});

describe("getHITLState", () => {
  it("reports a 'required' state for an awaiting_input task without a response", () => {
    const hitlDetail = createMockHITLDetail({
      response_received: false,
      task_instance: {
        dag_id: "test_dag",
        dag_run_id: "test_run",
        map_index: -1,
        state: "awaiting_input",
        task_id: "test_task",
      },
    } as Partial<HITLDetail>);

    // Empty params + non-approval options -> choice task; the point is that a parked
    // awaiting_input task is "required", not "noResponseReceived".
    expect(getHITLState(mockTranslate, hitlDetail)).toBe("state.choiceRequired");
  });

  it("reports no response received for a finished task without a response", () => {
    const hitlDetail = createMockHITLDetail({
      response_received: false,
      task_instance: {
        dag_id: "test_dag",
        dag_run_id: "test_run",
        map_index: -1,
        state: "success",
        task_id: "test_task",
      },
    } as Partial<HITLDetail>);

    expect(getHITLState(mockTranslate, hitlDetail)).toBe("state.noResponseReceived");
  });
});

describe("getHITLParamsDict", () => {
  it("correctly types object parameters as 'object' instead of 'string'", () => {
    const hitlDetail = createMockHITLDetail({
      params: {
        objectParam: { key: "value", nested: { data: 123 } },
      },
    });

    const searchParams = new URLSearchParams();
    const paramsDict = getHITLParamsDict(hitlDetail, mockTranslate, searchParams);

    expect(paramsDict.objectParam?.schema.type).toBe("object");
    expect(paramsDict.objectParam?.value).toEqual({ key: "value", nested: { data: 123 } });
  });
});
