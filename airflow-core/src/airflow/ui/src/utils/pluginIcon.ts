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

/** A plugin contribution that may carry its own icon: a view, a React app or an action. */
type IconBearing = {
  icon?: string | null;
  icon_dark_mode?: string | null;
};

/**
 * The icon URL to render for a plugin contribution, or `undefined` to fall back to a default.
 *
 * `icon_dark_mode` is optional, so a plugin supplying only `icon` uses it in both themes.
 */
export const getPluginIconSrc = (
  contribution: IconBearing,
  colorMode: string | undefined,
): string | undefined => {
  const preferred =
    colorMode === "dark" && contribution.icon_dark_mode !== undefined && contribution.icon_dark_mode !== null
      ? contribution.icon_dark_mode
      : contribution.icon;

  return preferred ?? undefined;
};
