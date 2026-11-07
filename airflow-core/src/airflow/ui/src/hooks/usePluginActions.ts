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
import { usePluginServiceGetPlugins } from "openapi/queries";

import type { PluginActionDestination, PluginActionResponse } from "src/components/PluginActions";

import { usePluginAppliesToContext } from "src/hooks/usePluginAppliesToContext";
import { type AppliesToContext, hasAppliesToCriteria } from "src/utils/pluginAppliesTo";

export type PluginActionsForDestination = {
  actions: Array<PluginActionResponse>;
  routeContext: AppliesToContext;
};

/**
 * The plugin actions for one destination, plus the route context their scoping is judged
 * against.
 *
 * Call this once per page and thread the result down to each surface -- never per table row.
 * `usePluginAppliesToContext` mounts three query observers, so a row-level call would stand up
 * a hundred and fifty of them on a full page of results. The context is only enabled when some
 * action for this destination is actually scoped, so the common case issues no requests.
 */
export const usePluginActions = (destination: PluginActionDestination): PluginActionsForDestination => {
  const { data } = usePluginServiceGetPlugins();

  const actions = (data?.plugins ?? [])
    .flatMap((plugin) => plugin.ui_actions ?? [])
    .filter((action) => action.destination === destination);

  const routeContext = usePluginAppliesToContext(actions.some(hasAppliesToCriteria));

  return { actions, routeContext };
};
