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
import { useCallback } from "react";
import { useHotkeys } from "react-hotkeys-hook";

import type { ArrowKey, NavigationDirection, NavigationMode } from "./types";

const ARROW_KEYS = ["ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"] as const;
const JUMP_KEYS = ["shift+ArrowDown", "shift+ArrowUp", "shift+ArrowLeft", "shift+ArrowRight"] as const;

type Props = {
  enabled?: boolean;
  mode: NavigationMode;
  onNavigate: (direction: NavigationDirection, isJump?: boolean) => void;
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

const isValidDirectionForMode = (direction: NavigationDirection, mode: NavigationMode): boolean => {
  switch (mode) {
    case "grid":
      return true;
    case "run":
      return direction === "left" || direction === "right";
    case "task":
      return direction === "down" || direction === "up";
    default:
      return false;
  }
};

export const useKeyboardNavigation = ({ enabled = true, mode, onNavigate }: Props) => {
  const handleNormalKeyPress = useCallback(
    (event: KeyboardEvent) => {
      if (!enabled) {
        return;
      }

      const direction = mapKeyToDirection(event.key as ArrowKey);

      if (!isValidDirectionForMode(direction, mode)) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();

      onNavigate(direction, false);
    },
    [enabled, mode, onNavigate],
  );

  const handleJumpKeyPress = useCallback(
    (event: KeyboardEvent) => {
      if (!enabled) {
        return;
      }

      const direction = mapKeyToDirection(event.key as ArrowKey);

      if (!isValidDirectionForMode(direction, mode)) {
        return;
      }

      event.preventDefault();
      event.stopPropagation();

      onNavigate(direction, true);
    },
    [enabled, mode, onNavigate],
  );

  useHotkeys(
    ARROW_KEYS.join(","),
    handleNormalKeyPress,
    {
      enabled,
      enableOnFormTags: false,
      preventDefault: true,
    },
    [enabled, mode, onNavigate],
  );

  useHotkeys(
    JUMP_KEYS.join(","),
    handleJumpKeyPress,
    {
      enabled,
      enableOnFormTags: false,
      preventDefault: true,
    },
    [enabled, mode, onNavigate],
  );

  return {
    handleKeyPress: handleNormalKeyPress,
  };
};
