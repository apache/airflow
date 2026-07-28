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
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";

import type { ArrowKey, NavigationDirection } from "./types";

type Props = {
  enabled?: boolean;
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

export const useKeyboardNavigation = ({ enabled = true, onNavigate, onToggleGroup }: Props) => {
  const handleNormalKeyPress = (event: KeyboardEvent) => {
    const direction = mapKeyToDirection(event.key as ArrowKey);

    event.preventDefault();
    event.stopPropagation();

    onNavigate(direction);
  };

  const hotkeyOptions = { enabled, preventDefault: true };

  useShortcut({
    ...SHORTCUTS.navigation.navigateTasks,
    callback: handleNormalKeyPress,
    dependencies: [onNavigate],
    options: hotkeyOptions,
  });

  useShortcut({
    ...SHORTCUTS.navigation.toggleTaskGroup,
    callback: () => onToggleGroup?.(),
    dependencies: [onToggleGroup],
    options: hotkeyOptions,
  });
};
