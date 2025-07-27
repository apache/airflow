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
import { useCallback, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";

import type { NavigationDirection, UseNavigationProps, UseNavigationReturn } from "./types";
import { useKeyboardNavigation } from "./useKeyboardNavigation";
import { useNavigationCalculation } from "./useNavigationCalculation";
import { useNavigationIndices } from "./useNavigationIndices";
import { useNavigationMode } from "./useNavigationMode";

export const useNavigation = ({ enabled = true, runs, tasks }: UseNavigationProps): UseNavigationReturn => {
  const { dagId = "" } = useParams();
  const navigate = useNavigate();
  const [isNavigating, setIsNavigating] = useState(false);

  // Get current navigation state
  const { mode, setMode } = useNavigationMode();
  const currentIndices = useNavigationIndices({ runs, tasks });
  const { getNavigationTarget } = useNavigationCalculation({ mode, runs, tasks });

  // Handle navigation action
  const handleNavigation = useCallback(
    (direction: NavigationDirection, isJump: boolean = false) => {
      console.log("🚀 handleNavigation called:", { dagId, direction, enabled, isJump });
      console.log("📍 Current indices:", currentIndices);
      console.log("📊 Data:", { runsCount: runs.length, tasksCount: tasks.length });

      if (!enabled || !dagId) {
        console.log("❌ Navigation blocked - enabled:", enabled, "dagId:", dagId);

        return;
      }

      setIsNavigating(true);

      const target = getNavigationTarget(currentIndices, direction, isJump);

      console.log("🎯 Navigation target:", target);

      if (target.isValid) {
        console.log("✅ Navigating to:", target.path);
        navigate(target.path, { replace: true });
      } else {
        console.log("❌ Invalid navigation target");
      }

      // Reset navigation state after a short delay
      setTimeout(() => {
        setIsNavigating(false);
      }, 100);
    },
    [currentIndices, dagId, enabled, getNavigationTarget, navigate, runs.length, tasks.length],
  );

  // Setup keyboard navigation
  useKeyboardNavigation({
    enabled: enabled && Boolean(dagId),
    onNavigate: handleNavigation,
  });

  return {
    currentIndices,
    enabled: enabled && Boolean(dagId),
    handleNavigation,
    isNavigating,
    mode,
    setMode,
    setNavigationEnabled: () => {
      // Keep for compatibility but not used
    },
  };
};
