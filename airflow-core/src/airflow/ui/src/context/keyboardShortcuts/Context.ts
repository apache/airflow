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
import { createContext } from "react";

// Display order of shortcut categories in the help modal.
export const SHORTCUT_CATEGORIES = [
  "global",
  "navigation",
  "dagView",
  "search",
  "filters",
  "logs",
  "code",
  "runActions",
] as const;

export type ShortcutCategory = (typeof SHORTCUT_CATEGORIES)[number];

export type ShortcutEntry = {
  readonly category: ShortcutCategory;
  readonly description: string;
  readonly id: string;
  readonly keys: ReadonlyArray<string>;
};

export type ShortcutRegistryContextValue = {
  readonly register: (entry: ShortcutEntry) => void;
  readonly shortcuts: ReadonlyArray<ShortcutEntry>;
  readonly unregister: (id: string) => void;
};

// No-op default so components using shortcuts can render without the provider
// (e.g. in isolated unit tests). The hotkeys still work; they just aren't
// listed in the help dialog. The real app wraps the tree in
// ShortcutRegistryProvider, so the dialog is populated there.
const NOOP_REGISTRY: ShortcutRegistryContextValue = {
  register: () => undefined,
  shortcuts: [],
  unregister: () => undefined,
};

export const ShortcutRegistryContext = createContext<ShortcutRegistryContextValue>(NOOP_REGISTRY);
