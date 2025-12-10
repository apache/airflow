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
import { useCallback, useEffect } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useDebouncedCallback } from "use-debounce";

import type { ArrowKey, NavigationDirection } from "./types";

const ARROW_KEYS = ["shift+ArrowDown", "shift+ArrowUp", "shift+ArrowLeft", "shift+ArrowRight"] as const;

type Props = {
  enabled?: boolean;
  onCommit?: () => void;
  onNavigate: (direction: NavigationDirection) => void;
  onToggleGroup?: () => void;
};

const mapKeyToDirection = (key: ArrowKey): NavigationDirection => {
  switch (key) {
    case "ArrowDown":
      return "down";
    case "ArrowLeft":
      return "left";
    case "ArrowRight":
      return "right";
    case "ArrowUp":
      return "up";
    default:
      return "down";
  }
};

const isArrowKey = (key: string): key is ArrowKey =>
  key === "ArrowDown" || key === "ArrowUp" || key === "ArrowLeft" || key === "ArrowRight";

export const useKeyboardNavigation = ({ enabled = true, onCommit, onNavigate, onToggleGroup }: Props) => {
  // Handle keydown: preview navigation (instant highlight)
  const handleKeyDown = useCallback(
    (event: KeyboardEvent) => {
      const direction = mapKeyToDirection(event.key as ArrowKey);

      event.preventDefault();
      event.stopPropagation();

      onNavigate(direction);
    },
    [onNavigate],
  );

  const hotkeyOptions = { enabled, preventDefault: true };

  useHotkeys(ARROW_KEYS.join(","), handleKeyDown, hotkeyOptions, [onNavigate]);

  useHotkeys("space", () => onToggleGroup?.(), hotkeyOptions, [onToggleGroup]);

  const debouncedCommit = useDebouncedCallback(() => {
    onCommit?.();
  }, 500);

  // Handle keyup: commit navigation (URL update)
  useEffect(() => {
    if (!enabled || !onCommit) {
      return undefined;
    }

    const handleKeyUp = (event: KeyboardEvent) => {
      // Only commit when shift + arrow key is released
      if (isArrowKey(event.key)) {
        debouncedCommit();
      }
    };

    globalThis.addEventListener("keyup", handleKeyUp);

    return () => {
      globalThis.removeEventListener("keyup", handleKeyUp);
    };
  }, [enabled, onCommit, debouncedCommit]);
};
