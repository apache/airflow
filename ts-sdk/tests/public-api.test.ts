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
import { listRegisteredTasks, registerTask, VariableNotFoundError } from "../src/index.js";
import { clearRegistry } from "../src/registry.js";

describe("public API", () => {
  it("exports task registration helpers", () => {
    clearRegistry();
    registerTask("public_api_task", async () => undefined);
    expect(listRegisteredTasks()).toEqual(["public_api_task"]);
  });

  it("exports public error classes", () => {
    const err = new VariableNotFoundError("missing");
    expect(err).toBeInstanceOf(Error);
    expect(err.name).toBe("VariableNotFoundError");
    expect(err.key).toBe("missing");
  });
});
