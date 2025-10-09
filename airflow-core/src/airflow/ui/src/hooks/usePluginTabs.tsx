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
import type { ReactNode } from "react";
import { LuPlug } from "react-icons/lu";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { ExternalViewResponse, ReactAppResponse } from "openapi/requests/types.gen";
import { useColorMode } from "src/context/colorMode";

type TabPlugin = {
  icon: ReactNode;
  label: string;
  value: string;
};

export const usePluginTabs = (destination: string): Array<TabPlugin> => {
  const { colorMode } = useColorMode();
  const { data: pluginData } = usePluginServiceGetPlugins();

  // Get external views with the specified destination and ensure they have url_route
  const externalViews =
    pluginData?.plugins
      .flatMap((plugin) => [...plugin.external_views, ...plugin.react_apps])
      .filter(
        (view: ExternalViewResponse | ReactAppResponse) =>
          view.destination === destination && Boolean(view.url_route),
      ) ?? [];

  return externalViews.map((view) => {
    // Choose icon based on theme - prefer dark mode icon if available and in dark mode
    let iconSrc = view.icon;

    if (colorMode === "dark" && view.icon_dark_mode !== undefined && view.icon_dark_mode !== null) {
      iconSrc = view.icon_dark_mode;
    }

    const icon =
      iconSrc !== undefined && iconSrc !== null ? (
        <img alt={view.name} src={iconSrc} style={{ height: "1rem", width: "1rem" }} />
      ) : (
        <LuPlug />
      );

    return {
      icon,
      label: view.name,
      value: `plugin/${view.url_route}`,
    };
  });
};
