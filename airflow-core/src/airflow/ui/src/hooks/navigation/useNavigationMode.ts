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

export const useNavigationMode = (initialMode: NavigationMode = "grid") => {
  const [mode, setMode] = useState<NavigationMode>(initialMode);

  const setModeFromClick = useCallback((target: ClickTarget) => {
    const newMode = detectModeFromClick(target);

    setMode(newMode);
  }, []);

  return {
    mode,
    setMode: setModeFromClick,
  };
};
