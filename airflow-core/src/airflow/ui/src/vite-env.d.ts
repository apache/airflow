/* eslint-disable @typescript-eslint/consistent-type-definitions */
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

/// <reference types="vite/types/importMeta.d.ts" />
/// <reference types="vite/client" />

interface ImportMeta {
  readonly env: ImportMetaEnv;
}

// monaco-editor ships .d.ts only for `editor.api`; contribution side-effect imports have
// no typings of their own.
declare module "monaco-editor/esm/vs/editor/contrib/folding/browser/folding";
declare module "monaco-editor/esm/vs/base/browser/ui/codicons/codiconStyles";
