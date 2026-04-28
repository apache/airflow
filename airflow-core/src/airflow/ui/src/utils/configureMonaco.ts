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
import { loader, type Monaco } from "@monaco-editor/react";
import "monaco-editor/esm/vs/basic-languages/python/python.contribution";
import * as monaco from "monaco-editor/esm/vs/editor/editor.api.js";
import "monaco-editor/esm/vs/language/json/monaco.contribution";
import EditorWorker from "monaco-editor/esm/vs/editor/editor.worker?worker";
import JsonWorker from "monaco-editor/esm/vs/language/json/json.worker?worker";

type WorkerConstructor = new () => Worker;
type MonacoGlobal = {
  MonacoEnvironment?: {
    readonly getWorker: (workerId: string, label: string) => Worker;
  };
} & typeof globalThis;

const EditorWorkerConstructor = EditorWorker as unknown as WorkerConstructor;
const JsonWorkerConstructor = JsonWorker as unknown as WorkerConstructor;

const workersByLabel: Record<string, WorkerConstructor> = {
  json: JsonWorkerConstructor,
};

export const configureMonaco = () => {
  const monacoGlobal = globalThis as MonacoGlobal;

  monacoGlobal.MonacoEnvironment = {
    getWorker: (_workerId: string, label: string) => {
      const Worker = workersByLabel[label] ?? EditorWorkerConstructor;

      return new Worker();
    },
  };

  loader.config({ monaco: monaco as unknown as Monaco });
};
