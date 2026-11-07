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

import { shouldListReportError } from "./errorHandling";

describe("shouldListReportError", () => {
  it("leaves a missing level to the header that already names it", () => {
    expect(shouldListReportError({ status: 404 }, true)).toBe(false);
  });

  it("reports a 404 where no header accounts for it", () => {
    expect(shouldListReportError({ status: 404 }, false)).toBe(true);
  });

  // The level loaded; this list failed on its own, so nothing else is saying why.
  it("reports a failure that is the list's own", () => {
    expect(shouldListReportError({ status: 500 }, true)).toBe(true);
    expect(shouldListReportError(new Error("network"), true)).toBe(true);
  });
});
