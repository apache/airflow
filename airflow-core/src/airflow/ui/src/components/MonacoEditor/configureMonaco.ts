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
import codiconFontUrl from "monaco-editor/esm/vs/base/browser/ui/codicons/codicon/codicon.ttf?url";

type MonacoEnvironment = {
  readonly getWorker: (_moduleId: string, label: string) => Worker;
};

let configurationPromise: Promise<void> | undefined;

// Inject the codicon @font-face + base class rule with an absolute font URL. Monaco's own
// `codiconStyles` module imports `codicon.css` which references the font as `url(./codicon.ttf)`;
// when the project's `vite-plugin-css-injected-by-js` inlines that CSS into a `<style>` tag the
// relative URL ends up resolved against the page origin instead of the asset directory. Resolving
// the font URL through Vite's `?url` import sidesteps that — and avoids the phantom CSS-chunk
// preload the plugin leaves behind, which otherwise rejects the configureMonaco promise in
// production builds. Monaco still registers the per-icon `::before { content: ... }` rules at
// runtime via its theme service, so we only need the @font-face + base class here.
const injectCodiconStyles = () => {
  if (document.querySelector('style[data-airflow="codicon-font"]') !== null) {
    return;
  }
  const style = document.createElement("style");

  style.dataset.airflow = "codicon-font";
  style.textContent = `
    @font-face {
      font-family: "codicon";
      font-display: block;
      src: url("${codiconFontUrl}") format("truetype");
    }
    .codicon[class*="codicon-"] {
      font: normal normal normal 16px/1 codicon;
      display: inline-block;
      text-decoration: none;
      text-rendering: auto;
      text-align: center;
      text-transform: none;
      -webkit-font-smoothing: antialiased;
      -moz-osx-font-smoothing: grayscale;
      user-select: none;
      -webkit-user-select: none;
    }
  `;
  document.head.append(style);
};

const loadMonacoModules = async () => {
  injectCodiconStyles();

  // `editor.api` is API-only — also load the folding contribution so `editor.foldAll` /
  // `editor.unfoldAll` actions and the fold-gutter UI are actually registered. The CDN
  // bundle used to pull this in transitively; the local ESM `editor.api` does not.
  const monacoApi = Promise.all([
    import("monaco-editor/esm/vs/editor/editor.api"),
    import("monaco-editor/esm/vs/editor/contrib/folding/browser/folding"),
  ]).then(([api]) => api);

  const workerConstructors = Promise.all([
    import("monaco-editor/esm/vs/editor/editor.worker?worker").then((module) => module.default),
    import("monaco-editor/esm/vs/language/json/json.worker?worker").then((module) => module.default),
  ]);

  const languageContributions = Promise.all([
    import("monaco-editor/esm/vs/basic-languages/python/python.contribution"),
    import("monaco-editor/esm/vs/language/json/monaco.contribution"),
  ]);

  const [monaco, [editorWorker, jsonWorker]] = await Promise.all([
    monacoApi,
    workerConstructors,
    languageContributions,
  ]);

  return { editorWorker, jsonWorker, monaco };
};

export const configureMonaco = () => {
  if (configurationPromise !== undefined) {
    return configurationPromise;
  }

  configurationPromise = loadMonacoModules()
    .then(({ editorWorker, jsonWorker, monaco }) => {
      Reflect.set(globalThis, "MonacoEnvironment", {
        getWorker: (_moduleId: string, label: string) =>
          label === "json" ? new jsonWorker() : new editorWorker(),
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
