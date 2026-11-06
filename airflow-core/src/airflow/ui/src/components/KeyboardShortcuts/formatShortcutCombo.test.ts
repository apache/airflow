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
import { describe, expect, it } from "vitest";

import { formatShortcutCombo } from "./formatShortcutCombo";

describe("formatShortcutCombo", () => {
  it.each([
    ["w", "⌘", "W"],
    ["mod+k", "⌘", "⌘ K"],
    ["mod+k", "Ctrl", "Ctrl K"],
    ["mod+shift+f", "⌘", "⌘ ⇧ F"],
    ["mod+ArrowUp", "Ctrl", "Ctrl ↑"],
    ["shift+ArrowDown", "⌘", "⇧ ↓"],
    ["space", "⌘", "Space"],
    ["/", "⌘", "/"],
  ])("formats %s (meta=%s) as %s", (combo, metaKey, expected) => {
    expect(formatShortcutCombo(combo, metaKey)).toBe(expected);
  });

  it("renders the ? alias for the help shortcut regardless of casing", () => {
    expect(formatShortcutCombo("shift+Slash", "⌘")).toBe("?");
    expect(formatShortcutCombo("shift+slash", "Ctrl")).toBe("?");
  });
});
