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
import { afterEach, describe, expect, it } from "vitest";

import type { ReactAppResponse } from "openapi/requests/types.gen";

import { loadPlugin } from "./ReactPlugin";

const pluginA = { bundle_url: "http://localhost/a.js", name: "PluginA" } as ReactAppResponse;
const pluginB = { bundle_url: "http://localhost/b.js", name: "PluginB" } as ReactAppResponse;
const componentA = () => null;

describe("loadPlugin", () => {
  afterEach(() => {
    for (const key of ["AirflowPlugin", pluginA.name, pluginB.name]) {
      (globalThis as Record<string, unknown>)[key] = undefined;
    }
  });

  it("does not let a malformed bundle inherit a previously-loaded plugin's component", async () => {
    // A well-formed bundle sets globalThis.AirflowPlugin on import; a malformed one sets nothing.
    await loadPlugin(pluginA, () => {
      (globalThis as Record<string, unknown>).AirflowPlugin = componentA;

      return Promise.resolve();
    });
    const { default: component } = await loadPlugin(pluginB, () => Promise.resolve());

    expect(component).not.toBe(componentA);
  });
});
