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
import type { Keys } from "react-hotkeys-hook";

import type { ShortcutCategory } from "./Context";

/**
 * Static definition of a keyboard shortcut: its key combination, the category it
 * belongs to in the help modal, and the `common` i18n key for its description.
 *
 * The runtime callback and any dynamic options (e.g. `enabled`) stay at the call
 * site, since they depend on component state.
 */
export type ShortcutDefinition = {
  readonly category: ShortcutCategory;
  readonly descriptionKey: string;
  readonly keys: Keys;
};

// Every category must be present, and each entry's `category` field must match
// the group it sits in — both are enforced at compile time.
type CategorizedShortcuts = {
  readonly [Category in ShortcutCategory]: Record<
    string,
    { readonly category: Category } & ShortcutDefinition
  >;
};

/**
 * Single source of truth for every keyboard shortcut in the UI, grouped by the
 * category shown in the help modal.
 *
 * Each `useShortcut` call spreads one of these entries (e.g.
 * `...SHORTCUTS.logs.toggleWrap`) instead of redeclaring its keys, category and
 * description inline, so maintainers can see every shortcut that already exists
 * in one place and avoid clashing key bindings. Description text lives in
 * `common.json` under the referenced `descriptionKey`.
 */
export const SHORTCUTS = {
  code: {
    toggleFullscreen: {
      category: "code",
      descriptionKey: "shortcuts.descriptions.toggleFullscreen",
      keys: "f",
    },
    toggleWrap: { category: "code", descriptionKey: "shortcuts.descriptions.toggleWrap", keys: "w" },
  },
  dagView: {
    toggleGraphGrid: {
      category: "dagView",
      descriptionKey: "shortcuts.descriptions.toggleGraphGrid",
      keys: "g",
    },
  },
  filters: {
    openGraphFilters: {
      category: "filters",
      descriptionKey: "shortcuts.descriptions.openGraphFilters",
      keys: "mod+shift+f",
    },
  },
  global: {
    showHelp: {
      category: "global",
      descriptionKey: "shortcuts.descriptions.showHelp",
      keys: "shift+Slash",
    },
  },
  logs: {
    downloadLogs: { category: "logs", descriptionKey: "shortcuts.descriptions.downloadLogs", keys: "d" },
    focusLogSearch: {
      category: "logs",
      descriptionKey: "shortcuts.descriptions.focusLogSearch",
      keys: "/",
    },
    scrollBottom: {
      category: "logs",
      descriptionKey: "shortcuts.descriptions.scrollBottom",
      keys: "mod+ArrowDown",
    },
    scrollTop: { category: "logs", descriptionKey: "shortcuts.descriptions.scrollTop", keys: "mod+ArrowUp" },
    toggleExpand: { category: "logs", descriptionKey: "shortcuts.descriptions.toggleExpand", keys: "e" },
    toggleFullscreen: {
      category: "logs",
      descriptionKey: "shortcuts.descriptions.toggleFullscreen",
      keys: "f",
    },
    toggleSource: { category: "logs", descriptionKey: "shortcuts.descriptions.toggleSource", keys: "s" },
    toggleTimestamp: {
      category: "logs",
      descriptionKey: "shortcuts.descriptions.toggleTimestamp",
      keys: "t",
    },
    toggleWrap: { category: "logs", descriptionKey: "shortcuts.descriptions.toggleWrap", keys: "w" },
  },
  navigation: {
    navigateTasks: {
      category: "navigation",
      descriptionKey: "shortcuts.descriptions.navigateTasks",
      keys: ["shift+ArrowDown", "shift+ArrowUp", "shift+ArrowLeft", "shift+ArrowRight"],
    },
    toggleTaskGroup: {
      category: "navigation",
      descriptionKey: "shortcuts.descriptions.toggleTaskGroup",
      keys: "space",
    },
  },
  runActions: {
    clearRun: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.clearRun",
      keys: "shift+c",
    },
    clearTaskInstance: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.clearTaskInstance",
      keys: "shift+c",
    },
    markRunFailed: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.markRunFailed",
      keys: "shift+f",
    },
    markRunSuccess: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.markRunSuccess",
      keys: "shift+s",
    },
    markTaskFailed: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.markTaskFailed",
      keys: "shift+f",
    },
    markTaskGroupFailed: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.markTaskGroupFailed",
      keys: "shift+f",
    },
    markTaskGroupSuccess: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.markTaskGroupSuccess",
      keys: "shift+s",
    },
    markTaskSuccess: {
      category: "runActions",
      descriptionKey: "shortcuts.descriptions.markTaskSuccess",
      keys: "shift+s",
    },
  },
  search: {
    focusFilterSearch: {
      category: "search",
      descriptionKey: "shortcuts.descriptions.focusFilterSearch",
      keys: "mod+k",
    },
    focusSearch: {
      category: "search",
      descriptionKey: "shortcuts.descriptions.focusSearch",
      keys: "mod+k",
    },
    searchDags: { category: "search", descriptionKey: "shortcuts.descriptions.searchDags", keys: "mod+k" },
  },
} satisfies CategorizedShortcuts;
