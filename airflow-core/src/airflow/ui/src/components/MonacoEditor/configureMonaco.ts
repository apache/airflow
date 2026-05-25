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
  readonly getWorker: (_moduleId: string, label: string) => Worker;
};

let configurationPromise: Promise<void> | undefined;

const loadMonacoModules = async () => {
  // `editor.api` is API-only — also load the folding contribution so `editor.foldAll` /
  // `editor.unfoldAll` actions and the fold-gutter UI are actually registered, and the
  // codicon styles so the gutter glyph (the `>` arrow) renders instead of an empty box.
  // The CDN bundle used to pull these in transitively; the local ESM `editor.api` does not.
  const monacoApi = Promise.all([
    import("monaco-editor/esm/vs/editor/editor.api"),
    import("monaco-editor/esm/vs/editor/contrib/folding/browser/folding"),
    import("monaco-editor/esm/vs/base/browser/ui/codicons/codiconStyles"),
  ]).then(([api]) => api);

  // Resolve the workers as plain URLs (not Vite `?worker` constructors). In dev mode
  // the SPA shell is served by the airflow api-server while Vite serves assets on a
  // different origin, and `new Worker(crossOriginUrl, { type: "module" })` is rejected
  // by the browser. Wrapping the cross-origin URL in a same-origin Blob shim that just
  // re-imports it sidesteps the restriction (CORS still permits the inner import). In
  // production the worker is same-origin and the shim is harmless.
  const workerUrls = Promise.all([
    import("monaco-editor/esm/vs/editor/editor.worker.js?url").then((module) => module.default),
    import("monaco-editor/esm/vs/language/json/json.worker.js?url").then((module) => module.default),
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
    .then(({ editorWorkerUrl, jsonWorkerUrl, monaco }) => {
      Reflect.set(globalThis, "MonacoEnvironment", {
        getWorker: (_moduleId: string, label: string) =>
          createWorkerFromUrl(label === "json" ? jsonWorkerUrl : editorWorkerUrl),
      } satisfies MonacoEnvironment);

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
