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

import type { NavigationIndices } from "../useGridNavigation";
import { NavigationCalculator, type ArrowKey } from "./NavigationCalculator";

const ARROW_KEYS = ["ArrowDown", "ArrowUp", "ArrowLeft", "ArrowRight"] as const;

type NavigationConfig = {
  CONTINUOUS_INTERVAL: number;
  LONG_PRESS_THRESHOLD: number;
};

type KeyboardState = {
  activeKey: ArrowKey | undefined;
  keyPressTime: number;
  lastContinuousIndices: NavigationIndices | undefined;
  timeouts: Array<NodeJS.Timeout>;
};

type Props = {
  applyPreviewEffect: (indices: NavigationIndices | undefined) => void;
  clearPreviewEffect: () => void;
  config: NavigationConfig;
  groupId: string;
  isGridFocused: boolean;
  navigateToPosition: (indices: NavigationIndices) => void;
  navigationCalculator: NavigationCalculator;
  resetNavigationState: () => void;
  runId: string;
  startContinuousMode: () => void;
  taskId: string;
};

export const useNavigationKeyboard = ({
  applyPreviewEffect,
  clearPreviewEffect,
  config,
  groupId,
  isGridFocused,
  navigateToPosition,
  navigationCalculator,
  resetNavigationState,
  runId,
  startContinuousMode,
  taskId,
}: Props) => {

  const stateRef = useRef<KeyboardState>({
    activeKey: undefined,
    keyPressTime: 0,
    lastContinuousIndices: undefined,
    timeouts: [],
  });

  const clearTimeouts = useCallback(() => {
    stateRef.current.timeouts.forEach(clearTimeout);
    stateRef.current.timeouts = [];
  }, []);

  const startContinuous = useCallback((
    key: ArrowKey,
    isJump: boolean,
    baseIndices: NavigationIndices
  ) => {
    startContinuousMode();

    const navigateStep = () => {
      const newIndices = navigationCalculator.calculateNewIndices(key, isJump, baseIndices);

      if (NavigationCalculator.areIndicesEqual(newIndices, baseIndices)) {
        return;
      }

      applyPreviewEffect(newIndices);
      stateRef.current.lastContinuousIndices = { ...newIndices };

      const timeout = setTimeout(navigateStep, config.CONTINUOUS_INTERVAL);

      stateRef.current.timeouts.push(timeout);
      Object.assign(baseIndices, newIndices);
    };

    navigateStep();
  }, [navigationCalculator, applyPreviewEffect, startContinuousMode, config.CONTINUOUS_INTERVAL]);

  const handleKeyDown = useCallback((key: ArrowKey, isJump: boolean) => {
    if (stateRef.current.activeKey === key) {
      return;
    }

    const current = navigationCalculator.getCurrentIndices();
    const newIndices = navigationCalculator.calculateNewIndices(key, isJump);

    if (NavigationCalculator.areIndicesEqual(newIndices, current)) {
      return;
    }

    clearTimeouts();

    if (isJump) {
      navigateToPosition(newIndices);

      return;
    }

    stateRef.current.activeKey = key;
    stateRef.current.keyPressTime = Date.now();

    const longPressTimeout = setTimeout(() => {
      if (stateRef.current.activeKey === key) {
        applyPreviewEffect(newIndices);
        startContinuous(key, isJump, { ...newIndices });
      }
    }, config.LONG_PRESS_THRESHOLD);

    stateRef.current.timeouts.push(longPressTimeout);
  }, [
    navigationCalculator,
    clearTimeouts,
    navigateToPosition,
    applyPreviewEffect,
    startContinuous,
    config.LONG_PRESS_THRESHOLD,
  ]);

  const handleKeyUp = useCallback((key: ArrowKey) => {
    if (stateRef.current.activeKey !== key) {
      return;
    }

    const pressDuration = Date.now() - stateRef.current.keyPressTime;
    const isShortPress = pressDuration < config.LONG_PRESS_THRESHOLD;

    clearTimeouts();
    clearPreviewEffect();
    resetNavigationState();

    if (isShortPress) {
      const newIndices = navigationCalculator.calculateNewIndices(key, false);

      navigateToPosition(newIndices);
    } else if (stateRef.current.lastContinuousIndices) {
      navigateToPosition(stateRef.current.lastContinuousIndices);
    }

    stateRef.current.activeKey = undefined;
    stateRef.current.lastContinuousIndices = undefined;
  }, [
    clearTimeouts,
    clearPreviewEffect,
    resetNavigationState,
    config.LONG_PRESS_THRESHOLD,
    navigationCalculator,
    navigateToPosition,
  ]);

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
    clearPreviewEffect();
    resetNavigationState();
    stateRef.current.activeKey = undefined;
    stateRef.current.lastContinuousIndices = undefined;
  }, [clearTimeouts, clearPreviewEffect, resetNavigationState]);

  useEffect(() => cleanup, [cleanup]);
  useEffect(() => { cleanup(); }, [runId, taskId, groupId, cleanup]);

};
