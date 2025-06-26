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
import type { InternalAxiosRequestConfig } from "axios";
import { afterEach, describe, it, vi, expect, beforeAll } from "vitest";

import { TOKEN_STORAGE_KEY, tokenHandler } from "./tokenHandler";

describe("TokenFlow Interceptor", () => {
  beforeAll(() => {
    Object.defineProperty(document, "cookie", {
      writable: true,
    });
  });

  it("Should read from the cookie, persist to the localStorage and remove from the cookie", () => {
    const token = "test-token";

    document.cookie = `_token=${token};`;

    const setItemMock = vi.spyOn(localStorage, "setItem");

    const headers = {};

    const config = { headers };

    const { headers: updatedHeaders } = tokenHandler(config as InternalAxiosRequestConfig);

    expect(setItemMock).toHaveBeenCalledOnce();
    expect(setItemMock).toHaveBeenCalledWith(TOKEN_STORAGE_KEY, token);
    expect(updatedHeaders).toEqual({ Authorization: `Bearer ${token}` });
    expect(document.cookie).toContain("_token=; expires=");
  });
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});
