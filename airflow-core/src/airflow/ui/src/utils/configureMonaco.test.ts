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
import { loader } from "@monaco-editor/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import { configureMonaco } from "./configureMonaco";

type MonacoEnvironmentWithGetWorker = {
  readonly getWorker: (workerId: string, label: string) => unknown;
};
type MonacoTestGlobal = {
  MonacoEnvironment?: MonacoEnvironmentWithGetWorker;
} & typeof globalThis;

vi.mock("@monaco-editor/react", () => ({
  loader: {
    config: vi.fn(),
  },
}));

vi.mock("monaco-editor/esm/vs/basic-languages/python/python.contribution", () => ({}));

vi.mock("monaco-editor/esm/vs/editor/editor.api.js", () => ({
  editor: {},
}));

vi.mock("monaco-editor/esm/vs/language/json/monaco.contribution", () => ({}));

vi.mock("monaco-editor/esm/vs/editor/editor.worker?worker", () => ({ default: AbortController }));

vi.mock("monaco-editor/esm/vs/language/json/json.worker?worker", () => ({
  default: EventTarget,
}));

const getConfiguredWorker = (label: string) =>
  ((globalThis as MonacoTestGlobal).MonacoEnvironment as MonacoEnvironmentWithGetWorker).getWorker("", label);

const getConstructorName = (value: unknown) => (value instanceof Object ? value.constructor.name : undefined);

describe("configureMonaco", () => {
  afterEach(() => {
    vi.clearAllMocks();
    (globalThis as MonacoTestGlobal).MonacoEnvironment = undefined;
  });

  it("configures Monaco to use the bundled editor package", () => {
    configureMonaco();

    expect(loader.config).toHaveBeenCalledWith({ monaco: { editor: {} } });
  });

  it("uses the bundled JSON worker for JSON models", () => {
    configureMonaco();

    const worker = getConfiguredWorker("json");

    expect(getConstructorName(worker)).toBe("EventTarget");
  });

  it("falls back to the bundled editor worker for other models", () => {
    configureMonaco();

    const worker = getConfiguredWorker("python");

    expect(getConstructorName(worker)).toBe("AbortController");
  });
});
