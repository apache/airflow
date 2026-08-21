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

import { patchPythonFStrings } from "./pythonFStrings";

type MonacoEnvironment = {
  readonly getWorker: (_moduleId: string, label: string) => Worker;
};

let configurationPromise: Promise<void> | undefined;

const loadMonacoModules = async () => {
  // The editor entry point is API-only; register the selected features and codicon styles
  // explicitly to keep the local bundle small.
  const monacoApi = Promise.all([
    import("monaco-editor/editor"),
    import("monaco-editor/features/codeAction/register"),
    import("monaco-editor/features/codelens/register"),
    import("monaco-editor/features/dropOrPasteInto/register"),
    import("monaco-editor/features/inlayHints/register"),
    import("monaco-editor/features/suggest/register"),
    import("monaco-editor/features/folding/register"),
    import("monaco-editor/features/find/register"),
    import("monaco-editor/features/codicon/register"),
  ]).then(([api]) => api);

  // Resolve the bundled worker URLs (`?worker&url` runs the worker through Vite's worker
  // pipeline — bundling all dependencies — and returns the resulting URL as a string,
  // unlike `?url` which would treat the file as a raw asset and either inline its source
  // as a data URL (assetsInlineLimit) or copy it without bundling its imports, leaving
  // unresolved bare specifiers at runtime). In dev mode the SPA shell is served by the
  // airflow api-server while Vite serves assets on a different origin, and
  // `new Worker(crossOriginUrl, { type: "module" })` is rejected by the browser.
  // Wrapping the cross-origin URL in a same-origin Blob shim that just re-imports it
  // sidesteps the restriction (CORS still permits the inner import). In production the
  // worker is same-origin and the shim is harmless.
  const workerUrls = Promise.all([
    import("monaco-editor/editor/editor.worker.js?worker&url").then((module) => module.default),
    import("monaco-editor/languages/features/json/json.worker.js?worker&url").then(
      (module) => module.default,
    ),
  ]);

  // The JSON feature registers its language as a side effect. Python is registered
  // manually below from its grammar module instead of importing its register module,
  // whose lazy tokens provider would overwrite our patched grammar on first use.
  // The runtime guard below fails loudly if the grammar export shape changes.
  const jsonContribution = import("monaco-editor/languages/features/json/register");
  const pythonGrammar = import("monaco-editor/languages/definitions/python/python");

  const [monaco, [editorWorkerUrl, jsonWorkerUrl], { conf: pythonConf, language: pythonLanguage }] =
    await Promise.all([monacoApi, workerUrls, pythonGrammar, jsonContribution]);

  return { editorWorkerUrl, jsonWorkerUrl, monaco, pythonConf, pythonLanguage };
};

const createWorkerFromUrl = (workerUrl: string): Worker => {
  const absoluteUrl = new URL(workerUrl, import.meta.url).href;
  const shim = `import ${JSON.stringify(absoluteUrl)};`;
  const blobUrl = URL.createObjectURL(new Blob([shim], { type: "text/javascript" }));

  return new Worker(blobUrl, { type: "module" });
};

export const configureMonaco = () => {
  if (configurationPromise !== undefined) {
    return configurationPromise;
  }

  configurationPromise = loadMonacoModules()
    .then(({ editorWorkerUrl, jsonWorkerUrl, monaco, pythonConf, pythonLanguage }) => {
      Reflect.set(globalThis, "MonacoEnvironment", {
        getWorker: (_moduleId: string, label: string) =>
          createWorkerFromUrl(label === "json" ? jsonWorkerUrl : editorWorkerUrl),
      } satisfies MonacoEnvironment);

      // Register Python with the patched grammar (triple-quoted f-string support). The
      // editor always sets `language="python"` explicitly, so no extensions/firstLine
      // auto-detection metadata is needed. Guard the internal grammar export shape: if a
      // monaco upgrade drops these, fail loudly here rather than silently disabling
      // Python highlighting (`setMonarchTokensProvider("python", undefined)`).
      // The `conf`/`language` types come from a hand-written ambient declaration, so
      // TypeScript believes they are always defined; this guard checks the real runtime
      // shape the types cannot vouch for.
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
      if (pythonConf === undefined || pythonLanguage === undefined) {
        throw new Error("monaco Python grammar module changed shape: missing `conf`/`language` export");
      }
      monaco.languages.register({ id: "python" });
      monaco.languages.setLanguageConfiguration("python", pythonConf);
      monaco.languages.setMonarchTokensProvider("python", patchPythonFStrings(pythonLanguage));

      loader.config({ monaco });
    })
    .catch((error: unknown) => {
      configurationPromise = undefined;
      // eslint-disable-next-line no-console
      console.error("Failed to configure Monaco editor", error);
      throw error;
    });

  return configurationPromise;
};
