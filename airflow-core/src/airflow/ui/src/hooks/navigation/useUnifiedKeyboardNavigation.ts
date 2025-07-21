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
import { useCallback, useRef, useEffect } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useNavigate, useSearchParams } from "react-router-dom";

import { useNavigationMode } from "src/context/navigationMode";

import type { ArrowKey } from "./NavigationCalculator";

const ARROW_KEYS = ["ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"] as const;

type NavigationConfig = {
  CONTINUOUS_INTERVAL: number;
  LONG_PRESS_THRESHOLD: number;
};

type KeyboardState = {
  activeKey: ArrowKey | undefined;
  keyPressTime: number;
  timeouts: Array<NodeJS.Timeout>;
};

type Props = {
  config: NavigationConfig;
  isGridFocused: boolean;
};

const getDirectionFromKey = (key: ArrowKey): "down" | "left" | "right" | "up" => {
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

export const useUnifiedKeyboardNavigation = ({ config, isGridFocused }: Props) => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const { getNavigationUrl, navigationState } = useNavigationMode();

  const stateRef = useRef<KeyboardState>({
    activeKey: undefined,
    keyPressTime: 0,
    timeouts: [],
  });

  const clearTimeouts = useCallback(() => {
    stateRef.current.timeouts.forEach(clearTimeout);
    stateRef.current.timeouts = [];
  }, []);

  const navigateToUrl = useCallback((url: string) => {
    navigate({
      pathname: url,
      search: searchParams.toString()
    }, { replace: true });
  }, [navigate, searchParams]);

  const handleNavigation = useCallback((key: ArrowKey) => {
    const direction = getDirectionFromKey(key);

    // Check if navigation is allowed in this direction for current mode
    if (direction === "left" || direction === "right") {
      if (!navigationState.canNavigateHorizontal) {
        return;
      }
    } else {
      if (!navigationState.canNavigateVertical) {
        return;
      }
    }

    const targetUrl = getNavigationUrl(direction);

    if (targetUrl !== undefined) {
      navigateToUrl(targetUrl);
    }
  }, [navigationState, getNavigationUrl, navigateToUrl]);

  const handleKeyDown = useCallback((key: ArrowKey, isJump: boolean) => {
    if (stateRef.current.activeKey === key) {
      return;
    }

    clearTimeouts();

    if (isJump) {
      handleNavigation(key);

      return;
    }

    stateRef.current.activeKey = key;
    stateRef.current.keyPressTime = Date.now();

    const longPressTimeout = setTimeout(() => {
      if (stateRef.current.activeKey === key) {
        // For long press, we start continuous navigation
        const continuousNavigation = () => {
          handleNavigation(key);

          const timeout = setTimeout(continuousNavigation, config.CONTINUOUS_INTERVAL);

          stateRef.current.timeouts.push(timeout);
        };

        continuousNavigation();
      }
    }, config.LONG_PRESS_THRESHOLD);

    stateRef.current.timeouts.push(longPressTimeout);
  }, [clearTimeouts, handleNavigation, config.LONG_PRESS_THRESHOLD, config.CONTINUOUS_INTERVAL]);

  const handleKeyUp = useCallback((key: ArrowKey) => {
    if (stateRef.current.activeKey !== key) {
      return;
    }

    const pressDuration = Date.now() - stateRef.current.keyPressTime;
    const isShortPress = pressDuration < config.LONG_PRESS_THRESHOLD;

    clearTimeouts();

    if (isShortPress) {
      handleNavigation(key);
    }

    stateRef.current.activeKey = undefined;
  }, [clearTimeouts, config.LONG_PRESS_THRESHOLD, handleNavigation]);

  useHotkeys(
    ARROW_KEYS.flatMap((key) => [key, `mod+${key}`]),
    (event) => {
      event.stopPropagation();

      if (event.type === 'keydown') {
        handleKeyDown(event.key as ArrowKey, event.metaKey || event.ctrlKey);
      } else {
        handleKeyUp(event.key as ArrowKey);
      }
    },
    {
      enabled: isGridFocused,
      keydown: true,
      keyup: true,
      preventDefault: true,
    }
  );

  const cleanup = useCallback(() => {
    clearTimeouts();
    stateRef.current.activeKey = undefined;
  }, [clearTimeouts]);

  useEffect(() => cleanup, [cleanup]);
};
