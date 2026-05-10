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
import "monaco-editor/esm/vs/basic-languages/python/python.contribution";
import * as monaco from "monaco-editor/esm/vs/editor/editor.api";
import editorWorkerUrl from "monaco-editor/esm/vs/editor/editor.worker?url";
import jsonWorkerUrl from "monaco-editor/esm/vs/language/json/json.worker?url";
import "monaco-editor/esm/vs/language/json/monaco.contribution";

type MonacoEnvironment = {
  readonly getWorkerUrl: (_moduleId: string, label: string) => string;
};

const createWorkerUrl = (workerUrl: string) => {
  const workerSource = `import ${JSON.stringify(new URL(workerUrl, import.meta.url).href)};`;

  return URL.createObjectURL(new Blob([workerSource], { type: "text/javascript" }));
};

const editorWorkerBlobUrl = createWorkerUrl(editorWorkerUrl);
const jsonWorkerBlobUrl = createWorkerUrl(jsonWorkerUrl);

Reflect.set(globalThis, "MonacoEnvironment", {
  getWorkerUrl: (_moduleId: string, label: string) =>
    label === "json" ? jsonWorkerBlobUrl : editorWorkerBlobUrl,
} satisfies MonacoEnvironment);

loader.config({ monaco });
