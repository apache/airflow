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

import { useConfigServiceGetConfigs } from "openapi/queries";
import type { AppBuilderMenuItemResponse } from "openapi/requests/types.gen";
import { useColorMode } from "src/context/colorMode";

type TabPlugin = {
  icon: ReactNode;
  label: string;
  value: string;
};

export const usePluginTabs = (destination: string): Array<TabPlugin> => {
  const { colorMode } = useColorMode();
  const { data: config } = useConfigServiceGetConfigs();

  const externalViews =
    config?.plugins_extra_menu_items?.filter(
      (item: AppBuilderMenuItemResponse) =>
        item.category?.toLowerCase() === destination,
    ) ?? [];

  return externalViews.map((view) => {
    // an icon can be provided for menu items, but for now we will just use the default
    const icon = <LuPlug />;

    return {
      icon,
      label: view.name,
      value: view.href,
    };
  });
};
