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
import { useRef, type ReactNode } from "react";

import type { RecentTasks } from "src/queries/useRecentTaskStateCounts";

import {
  DagsListCountsContext,
  type DagsListCountsValue,
  type RunStateCounts,
} from "./DagsListCountsContext";

/**
 * Carries the two polled count collections to the Dags list's cards and cells.
 *
 * They arrive from their own queries and change on every auto-refresh tick, so passing them
 * through `createColumns`/`createCardDef` would rebuild those renderers and give React a new
 * component type each tick — remounting every card and resetting their lazy-render latches.
 * Reading them from context keeps the renderers module-level constants that update in place.
 */
export const DagsListCountsProvider = ({
  children,
  recentTasks,
  runStateCounts,
}: {
  readonly children: ReactNode;
  readonly recentTasks: RecentTasks;
  readonly runStateCounts: RunStateCounts;
}) => {
  // Outlives the cards, so a card that remounts keeps whatever it had already revealed.
  const revealedKeys = useRef(new Set<string>());
  const value: DagsListCountsValue = { recentTasks, revealedKeys: revealedKeys.current, runStateCounts };

  return <DagsListCountsContext.Provider value={value}>{children}</DagsListCountsContext.Provider>;
};
