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
import { LuPlug } from "react-icons/lu";

import { useColorMode } from "src/context/colorMode";
import { getPluginIconSrc } from "src/utils/pluginIcon";

import type { PluginActionResponse } from "./types";

/**
 * A plugin action's icon, falling back to a generic plug when it ships none.
 *
 * The image is `alt=""` on purpose: the surrounding `IconButton` already carries the action
 * name as its `aria-label`, so naming it again here would announce it twice.
 */
export const PluginActionIcon = ({ action }: { readonly action: PluginActionResponse }) => {
  const { colorMode } = useColorMode();

  const iconSrc = getPluginIconSrc(action, colorMode);

  return iconSrc === undefined ? (
    <LuPlug />
  ) : (
    <img alt="" src={iconSrc} style={{ height: "1rem", width: "1rem" }} />
  );
};
