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
import { type FC, useRef, useState } from "react";

import type { PluginReactActionResponse } from "openapi/requests/types.gen";

import { IconButton } from "src/system-components";

import { loadPlugin, type PluginProps } from "src/pages/ReactPlugin";

import { PluginActionBoundary } from "./PluginActionBoundary";
import { PluginActionIcon } from "./PluginActionIcon";
import type { PluginActionButtonProps, PluginActionProps } from "./types";

/**
 * A plugin action that reveals a component of the plugin's own making.
 *
 * Core contributes the trigger button and nothing else: the plugin portals its own popover,
 * dialog or drawer, so it is not boxed into one shape. It gets `open` to follow, `onClose` to
 * dismiss itself, and `triggerRef` to anchor against -- a popover needs the button's rect, and
 * the button is ours.
 *
 * The component mounts on first open and then stays mounted, so it can animate out and keep
 * whatever state it had. Nothing is loaded for an action nobody clicks, which matters on a
 * table where the same action exists on every row.
 *
 * `loadPlugin` hands back the component, which is held in state and rendered directly. Going
 * through `ReactPlugin` instead would mean a Suspense boundary whose placeholder reflows the
 * action row, and a second reader of `globalThis[name]` -- which an action named `Image` or
 * `History` satisfies with the browser's own constructor rather than the bundle's component.
 */
export const PluginReactAction = ({
  action,
  buttonProps,
  pluginProps,
}: {
  readonly action: PluginReactActionResponse;
  readonly buttonProps?: PluginActionButtonProps;
  readonly pluginProps: PluginProps;
}) => {
  const [open, setOpen] = useState(false);
  const [pluginComponent, setPluginComponent] = useState<FC<PluginActionProps>>();
  const [isLoading, setIsLoading] = useState(false);
  const triggerRef = useRef<HTMLButtonElement>(null);

  const Plugin = pluginComponent;

  const onToggle = async () => {
    if (pluginComponent === undefined) {
      setIsLoading(true);

      const { default: loaded } = await loadPlugin(action);

      // Wrapped in a setter: a bare component would be called as a lazy initializer.
      setPluginComponent(() => loaded as FC<PluginActionProps>);
      setIsLoading(false);
    }

    setOpen((wasOpen) => !wasOpen);
  };

  return (
    <>
      <IconButton
        label={action.name}
        loading={isLoading}
        onClick={() => {
          void onToggle();
        }}
        ref={triggerRef}
        {...buttonProps}
      >
        <PluginActionIcon action={action} />
      </IconButton>
      {Plugin === undefined ? undefined : (
        <PluginActionBoundary name={action.name}>
          <Plugin {...pluginProps} onClose={() => setOpen(false)} open={open} triggerRef={triggerRef} />
        </PluginActionBoundary>
      )}
    </>
  );
};
