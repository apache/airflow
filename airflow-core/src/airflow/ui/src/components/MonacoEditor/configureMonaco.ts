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

type MonacoEnvironment = {
  readonly getWorkerUrl: (_moduleId: string, label: string) => string;
};

let configurationPromise: Promise<void> | undefined;

const createWorkerBlobUrl = (workerUrl: string) => {
  const workerSource = `import ${JSON.stringify(new URL(workerUrl, import.meta.url).href)};`;

  return URL.createObjectURL(new Blob([workerSource], { type: "text/javascript" }));
};

const loadMonacoModules = async () => {
  const monacoApi = import("monaco-editor/esm/vs/editor/editor.api");

  const workerUrls = Promise.all([
    import("monaco-editor/esm/vs/editor/editor.worker?url").then((module) => module.default),
    import("monaco-editor/esm/vs/language/json/json.worker?url").then((module) => module.default),
  ]);

  const languageContributions = Promise.all([
    import("monaco-editor/esm/vs/basic-languages/python/python.contribution"),
    import("monaco-editor/esm/vs/language/json/monaco.contribution"),
  ]);

  const [monaco, [editorWorkerUrl, jsonWorkerUrl]] = await Promise.all([
    monacoApi,
    workerUrls,
    languageContributions,
  ]);

  return { editorWorkerUrl, jsonWorkerUrl, monaco };
};

export const configureMonaco = () => {
  if (configurationPromise !== undefined) {
    return configurationPromise;
  }

  configurationPromise = loadMonacoModules().then(({ editorWorkerUrl, jsonWorkerUrl, monaco }) => {
    const editorWorkerBlobUrl = createWorkerBlobUrl(editorWorkerUrl);
    const jsonWorkerBlobUrl = createWorkerBlobUrl(jsonWorkerUrl);

    Reflect.set(globalThis, "MonacoEnvironment", {
      getWorkerUrl: (_moduleId: string, label: string) =>
        label === "json" ? jsonWorkerBlobUrl : editorWorkerBlobUrl,
    } satisfies MonacoEnvironment);

    loader.config({ monaco });
  });

  return configurationPromise;
};
