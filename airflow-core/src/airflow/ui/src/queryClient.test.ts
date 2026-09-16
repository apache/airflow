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
import axios from "axios";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Airflow can be served under a path prefix (`[api] base_url`), which the
// server renders into `<base href>`. Every API request must be resolved
// against it, otherwise requests land on the origin root, where a reverse
// proxy in front of Airflow may route them to an entirely different service.
const BASE_HREF = "/workflows/";

describe("API client base path", () => {
  beforeEach(() => {
    vi.resetModules();
    document.head.innerHTML = `<base href="${BASE_HREF}" />`;
  });

  afterEach(() => {
    document.head.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("prefixes requests issued while the app is initializing", async () => {
    const requested: Array<string> = [];

    vi.spyOn(axios, "request").mockImplementation((config) => {
      requested.push(String(config.url));

      return Promise.resolve({
        config,
        data: { git_version: null, version: "3.3.1" },
        headers: {},
        status: 200,
        statusText: "OK",
      });
    });

    // Importing the query client pulls in the app's initialization chain. That
    // chain includes i18n, which requests the version at module scope to build
    // a cache buster -- so this request is issued before any component mounts.
    await import("src/queryClient");

    await vi.waitFor(() => {
      expect(requested.length).toBeGreaterThan(0);
    });

    expect(requested.filter((url) => !url.startsWith(BASE_HREF))).toEqual([]);
  });
});
