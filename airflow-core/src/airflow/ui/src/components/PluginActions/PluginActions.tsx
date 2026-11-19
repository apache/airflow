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
import { type AppliesToContext, filterScopedViews, mergeAppliesToContext } from "src/utils/pluginAppliesTo";

import { usePluginActionsContext } from "./PluginActionsContext";
import { PluginLinkAction } from "./PluginLinkAction";
import { PluginReactAction } from "./PluginReactAction";
import { pluginActionContext } from "./pluginActionContext";
import type { PluginActionButtonProps, PluginActionEntity, PluginActionResponse } from "./types";

// Two plugins may both ship, say, a link action called "Runbook", and nothing forbids it -- only
// React action names are kept unique, because those collide on `globalThis`. Keying on the
// target as well keeps the list stable without requiring uniqueness we do not enforce.
const actionKey = (action: PluginActionResponse) =>
  `${action.action_type}:${action.name}:${action.action_type === "link" ? action.href : action.bundle_url}`;

/**
 * The plugin-contributed actions for one entity, rendered after its built-in ones.
 *
 * `actions` and `routeContext` come from `usePluginActions`. A header calls that hook itself
 * and passes them; a table row leaves them out and they come from the page's
 * `PluginActionsProvider`, which is what keeps the cell a module-level component instead of
 * one rebuilt -- and so remounted -- on every refresh tick.
 *
 * A deployment with no plugin actions returns before doing any per-row work.
 */
export const PluginActions = ({
  actions,
  buttonProps,
  entity,
  routeContext,
}: {
  readonly actions?: Array<PluginActionResponse>;
  /** Styling for the trigger buttons, so they match whatever they are sitting beside. */
  readonly buttonProps?: PluginActionButtonProps;
  readonly entity: PluginActionEntity;
  readonly routeContext?: AppliesToContext;
}) => {
  const fromContext = usePluginActionsContext();
  const resolved = actions ?? fromContext.actions;

  if (resolved.length === 0) {
    return undefined;
  }

  const { appliesTo, pluginProps, tokens } = pluginActionContext(entity);
  const visible = filterScopedViews(
    resolved,
    entity.destination,
    mergeAppliesToContext(routeContext ?? fromContext.routeContext, appliesTo),
  );

  return (
    <>
      {visible.map((action) =>
        action.action_type === "link" ? (
          <PluginLinkAction
            action={action}
            buttonProps={buttonProps}
            key={actionKey(action)}
            tokens={tokens}
          />
        ) : (
          <PluginReactAction
            action={action}
            buttonProps={buttonProps}
            key={actionKey(action)}
            pluginProps={pluginProps}
          />
        ),
      )}
    </>
  );
};
