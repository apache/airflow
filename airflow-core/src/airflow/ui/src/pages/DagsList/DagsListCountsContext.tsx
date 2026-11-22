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
import { createContext, useContext, useRef, type ReactNode } from "react";

import type { RecentTasks } from "src/queries/useRecentTaskStateCounts";

export type RunStateCounts = {
  readonly countsByDag: Record<string, Record<string, number> | undefined>;
  readonly isLoading: boolean;
  readonly stateCountLimit: number | undefined;
};

type DagsListCountsValue = {
  readonly recentTasks: RecentTasks;
  /** Dag ids whose cards have already revealed their lazily-rendered content. */
  readonly revealedKeys: Set<string>;
  readonly runStateCounts: RunStateCounts;
};

const DagsListCountsContext = createContext<DagsListCountsValue | undefined>(undefined);

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

export const useDagsListCounts = () => {
  const ctx = useContext(DagsListCountsContext);

  if (ctx === undefined) {
    throw new Error("Dags list cards and cells must be used inside <DagsListCountsProvider>");
  }

  return ctx;
};

/** The reveal cache when rendered inside the list, and `undefined` for a card rendered on its own. */
export const useRevealedDagCards = () => useContext(DagsListCountsContext)?.revealedKeys;
