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
import { useCallback, useEffect, useState } from "react";
import { useParams } from "react-router-dom";

import type { ClickTarget, NavigationMode } from "./types";

const detectModeFromClick = (target: ClickTarget): NavigationMode => {
  switch (target) {
    case "column":
      return "run";
    case "grid":
      return "grid";
    case "row":
      return "task";
    default:
      return "grid";
  }
};

const detectModeFromUrl = (pathname: string): NavigationMode => {
  if (pathname.includes("/runs/") && pathname.includes("/tasks/")) {
    return "grid";
  }
  if (pathname.includes("/runs/") && !pathname.includes("/tasks/")) {
    return "run";
  }
  if (pathname.includes("/tasks/") && !pathname.includes("/runs/")) {
    return "task";
  }

  return "grid";
};

export const useNavigationMode = (initialMode: NavigationMode = "grid") => {
  const [mode, setMode] = useState<NavigationMode>(initialMode);
  const { "*": wildcard } = useParams();

  // Auto-detect mode from URL
  useEffect(() => {
    if (typeof globalThis !== "undefined") {
      const detectedMode = detectModeFromUrl(globalThis.location.pathname);

      setMode(detectedMode);
    }
  }, [wildcard]);

  const setModeFromClick = useCallback((target: ClickTarget) => {
    const newMode = detectModeFromClick(target);

    setMode(newMode);
  }, []);

  return {
    mode,
    setMode: setModeFromClick,
  };
};
