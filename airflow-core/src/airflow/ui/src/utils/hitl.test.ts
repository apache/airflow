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

import { getHITLParamsDict } from "./hitl";

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
