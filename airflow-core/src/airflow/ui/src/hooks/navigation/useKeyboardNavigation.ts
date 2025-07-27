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

import type { ArrowKey, NavigationDirection } from "./types";

const ARROW_KEYS = ["ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"] as const;
const JUMP_KEYS = ["shift+ArrowDown", "shift+ArrowUp", "shift+ArrowLeft", "shift+ArrowRight"] as const;

type Props = {
  enabled?: boolean;
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

export const useKeyboardNavigation = ({ enabled = true, onNavigate }: Props) => {
  const handleKeyPress = useCallback(
    (event: KeyboardEvent) => {
      console.log("🔥 Key pressed:", event.key, "enabled:", enabled);

      if (!enabled) {
        console.log("❌ Navigation disabled");

        return;
      }

      const direction = mapKeyToDirection(event.key as ArrowKey);
      const isJump = event.shiftKey;

      console.log("✅ Navigating:", direction, "isJump:", isJump);

      event.preventDefault();
      event.stopPropagation();

      onNavigate(direction, isJump);
    },
    [enabled, onNavigate],
  );

  const handleJumpKeyPress = useCallback(
    (event: KeyboardEvent) => {
      console.log("🚀 Jump key pressed:", event.key, "enabled:", enabled);

      if (!enabled) {
        console.log("❌ Navigation disabled");

        return;
      }

      const direction = mapKeyToDirection(event.key as ArrowKey);

      console.log("✅ Jump navigating:", direction);

      event.preventDefault();
      event.stopPropagation();

      onNavigate(direction, true);
    },
    [enabled, onNavigate],
  );

  useHotkeys(
    ARROW_KEYS.join(","),
    handleKeyPress,
    {
      enabled,
      enableOnFormTags: false,
      preventDefault: true,
    },
    [enabled, onNavigate],
  );

  useHotkeys(
    JUMP_KEYS.join(","),
    handleJumpKeyPress,
    {
      enabled,
      enableOnFormTags: false,
      preventDefault: true,
    },
    [enabled, onNavigate],
  );

  return {
    handleKeyPress,
  };
};
