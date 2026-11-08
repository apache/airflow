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

import { handlers } from "./task_instances";

const TASK_INSTANCE_URL = "http://localhost:3000/api/v2/dags/dag_1/dagRuns/run_1/taskInstances/task_1";

const findMatchingHandlers = async (url: string) => {
  const matches = await Promise.all(
    handlers.map((handler) => handler.test({ request: new Request(url, { method: "GET" }) })),
  );

  return handlers.filter((_, index) => matches[index]);
};

describe("task instance mock handlers", () => {
  it("answer a mapped task instance by its map index", async () => {
    expect(await findMatchingHandlers(`${TASK_INSTANCE_URL}/0`)).toHaveLength(1);
  });

  // A matching handler that returns nothing passes the request through to the network without
  // msw reporting it as unhandled, so sub-resources must not match at all.
  it.each(["links", "tries"])("leave the %s sub-resource unmatched", async (subResource) => {
    expect(await findMatchingHandlers(`${TASK_INSTANCE_URL}/${subResource}`)).toHaveLength(0);
  });
});
