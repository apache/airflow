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

// Symbols for keys whose name is not the desired display label.
const KEY_SYMBOLS: Record<string, string> = {
  alt: "⌥",
  arrowdown: "↓",
  arrowleft: "←",
  arrowright: "→",
  arrowup: "↑",
  ctrl: "Ctrl",
  shift: "⇧",
  slash: "/",
  space: "Space",
};

// Combos bound to one key for matching reasons but better shown as a single glyph.
// `shift+Slash` is how react-hotkeys-hook reliably matches the "?" key.
const COMBO_ALIASES: Record<string, string> = {
  "shift+slash": "?",
};

/**
 * Turn a `react-hotkeys-hook` combo such as `"mod+shift+f"` into a human-readable
 * label like `"⌘ ⇧ F"`. `metaKey` is the platform's meta key symbol (`⌘`/`Ctrl`).
 */
export const formatShortcutCombo = (combo: string, metaKey: string): string => {
  const alias = COMBO_ALIASES[combo.toLowerCase()];

  if (alias !== undefined) {
    return alias;
  }

  return combo
    .split("+")
    .map((part) => {
      const lower = part.toLowerCase();

      if (lower === "mod") {
        return metaKey;
      }

      if (lower in KEY_SYMBOLS) {
        return KEY_SYMBOLS[lower];
      }

      return part.length === 1 ? part.toUpperCase() : part;
    })
    .join(" ");
};
