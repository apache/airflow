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
// Plugin UMD bundles expect the host's React object as a global.
// eslint-disable-next-line @typescript-eslint/no-restricted-imports
import React from "react";

import * as ChakraUI from "@chakra-ui/react";
import * as EmotionReact from "@emotion/react";
import * as ReactDOM from "react-dom";
import * as ReactRouterDOM from "react-router-dom";
import * as ReactJSXRuntime from "react/jsx-runtime";
import { afterEach, describe, expect, it } from "vitest";

import { registerPluginDependencies } from "./registerPluginDependencies";

const dependencyGlobals = [
  "React",
  "ReactDOM",
  "ReactJSXRuntime",
  "ReactRouterDOM",
  "ChakraUI",
  "EmotionReact",
] as const;
const originalGlobals = new Map(dependencyGlobals.map((key) => [key, Reflect.get(globalThis, key)]));

afterEach(() => {
  for (const key of dependencyGlobals) {
    Reflect.set(globalThis, key, originalGlobals.get(key));
  }
});

describe("registerPluginDependencies", () => {
  it("publishes the host dependencies expected by plugin bundles", () => {
    registerPluginDependencies();

    expect(Reflect.get(globalThis, "React")).toBe(React);
    expect(Reflect.get(globalThis, "ReactDOM")).toBe(ReactDOM);
    expect(Reflect.get(globalThis, "ReactJSXRuntime")).toBe(ReactJSXRuntime);
    expect(Reflect.get(globalThis, "ReactRouterDOM")).toBe(ReactRouterDOM);
    expect(Reflect.get(globalThis, "ChakraUI")).toBe(ChakraUI);
    expect(Reflect.get(globalThis, "EmotionReact")).toBe(EmotionReact);
  });
});
