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
import { renderHook } from "@testing-library/react";
import type { PropsWithChildren } from "react";
import { describe, expect, it, vi } from "vitest";

import {
  ShortcutRegistryProvider,
  type ShortcutEntry,
  useShortcutRegistry,
} from "src/context/keyboardShortcuts";

import { useShortcut } from "./useShortcut";

const mockTranslate = vi.fn((key: string) => {
  const translations: Record<string, string> = {
    "shortcuts.descriptions.toggleWrap": "Toggle Wrap",
  };

  return translations[key] ?? key;
});

vi.mock("react-i18next", () => ({
  useTranslation: () => ({
    i18n: { language: "en" },
    // eslint-disable-next-line id-length
    t: mockTranslate,
  }),
}));

const wrapper = ({ children }: PropsWithChildren) => (
  <ShortcutRegistryProvider>{children}</ShortcutRegistryProvider>
);

const renderShortcut = (initialEnabled = true) =>
  renderHook(
    ({ enabled }: { enabled: boolean }): ReadonlyArray<ShortcutEntry> => {
      useShortcut({
        callback: vi.fn(),
        category: "logs",
        descriptionKey: "shortcuts.descriptions.toggleWrap",
        keys: "w",
        options: { enabled },
      });

      return useShortcutRegistry().shortcuts;
    },
    { initialProps: { enabled: initialEnabled }, wrapper },
  );

describe("useShortcut", () => {
  it("registers the shortcut while mounted", () => {
    const { result } = renderShortcut();

    expect(result.current).toHaveLength(1);
    expect(result.current[0]).toMatchObject({
      category: "logs",
      description: "Toggle Wrap",
      keys: ["w"],
    });
  });

  it("removes the shortcut when it becomes disabled", () => {
    const { rerender, result } = renderShortcut(true);

    expect(result.current).toHaveLength(1);
    rerender({ enabled: false });
    expect(result.current).toHaveLength(0);
  });

  it("does not register a disabled shortcut", () => {
    const { result } = renderShortcut(false);

    expect(result.current).toHaveLength(0);
  });

  it("registers each combo of a multi-key shortcut for display", () => {
    const { result } = renderHook(
      (): ReadonlyArray<ShortcutEntry> => {
        useShortcut({
          callback: vi.fn(),
          category: "navigation",
          descriptionKey: "shortcuts.descriptions.navigateTasks",
          keys: ["shift+ArrowUp", "shift+ArrowDown"],
        });

        return useShortcutRegistry().shortcuts;
      },
      { wrapper },
    );

    expect(result.current).toHaveLength(1);
    expect(result.current[0]?.keys).toEqual(["shift+ArrowUp", "shift+ArrowDown"]);
  });
});
