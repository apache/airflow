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
import { createContext, useContext, type ReactNode } from "react";

import type { PluginActionsForDestination } from "src/hooks/usePluginActions";

// Frozen at module level so a table rendered outside a provider keeps a stable identity.
const NO_ACTIONS: PluginActionsForDestination = { actions: [], routeContext: { isLoading: false } };

const PluginActionsContext = createContext<PluginActionsForDestination>(NO_ACTIONS);

/**
 * Carries a table's plugin actions to its row cells.
 *
 * Passing them through the column factory instead would rebuild the columns whenever they
 * change -- and when any action is scoped, `routeContext` holds the page's own polled queries,
 * so that is every auto-refresh tick. `flexRender` would then see a new component type each
 * time and remount the actions cell, closing an open plugin surface along with whatever
 * dialog sat beside it.
 */
export const PluginActionsProvider = ({
  children,
  value,
}: {
  readonly children: ReactNode;
  readonly value: PluginActionsForDestination;
}) => <PluginActionsContext.Provider value={value}>{children}</PluginActionsContext.Provider>;

export const usePluginActionsContext = (): PluginActionsForDestination => useContext(PluginActionsContext);
