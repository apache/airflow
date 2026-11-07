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
import { Box } from "@chakra-ui/react";
import { LuExternalLink } from "react-icons/lu";
import { Link as ReactRouterLink, useLocation } from "react-router-dom";

import type { PluginLinkActionResponse } from "openapi/requests/types.gen";

import { IconButton } from "src/system-components";

import { getInternalRouteTo, getSafeExternalUrl } from "src/utils/links";
import { applyPluginTemplate, type PluginTemplateTokens } from "src/utils/pluginTemplate";

import { PluginActionIcon } from "./PluginActionIcon";
import type { PluginActionButtonProps } from "./types";

/**
 * A plugin action that opens a URL.
 *
 * The resolved href is run through `getSafeExternalUrl` even though the plugin is trusted,
 * because the template interpolates Dag-author-controlled identifiers into it -- the final
 * string is not purely the plugin's. An href it rejects renders nothing at all rather than a
 * dead button.
 *
 * "External" here means *not an Airflow UI route*, which is a wider net than cross-origin: a
 * plugin's own backend page shares this origin but still leaves the single-page app, so
 * following it in place would drop the user out of the UI and lose their position. Everything
 * external opens in a new tab and is badged with a small arrow in the icon's top right; only a
 * real UI route navigates in place, where no marker is warranted because nothing is left
 * behind. The badge is positioned out of flow so marking an action costs no layout.
 */
export const PluginLinkAction = ({
  action,
  buttonProps,
  tokens,
}: {
  readonly action: PluginLinkActionResponse;
  readonly buttonProps?: PluginActionButtonProps;
  readonly tokens: PluginTemplateTokens;
}) => {
  const location = useLocation();
  const href = getSafeExternalUrl(applyPluginTemplate(action.href, tokens));

  if (href === undefined) {
    return undefined;
  }

  const internalTo = getInternalRouteTo(href, location);

  if (internalTo === undefined) {
    return (
      <IconButton asChild label={action.name} position="relative" {...buttonProps}>
        <a href={href} rel="noopener noreferrer" target="_blank">
          <PluginActionIcon action={action} />
          {/* Pinned to the button's corner rather than the icon's: an icon-relative badge
              covers most of the upper-right of the 1rem glyph it is annotating, whereas the
              button's padding is empty space. */}
          <Box color="fg.muted" position="absolute" right="2px" top="2px">
            {/* Sized inline because Chakra's base stylesheet sizes every `svg` to 1em, which
                beats react-icons' own width/height attributes and would render this badge as
                large as the icon it is annotating. */}
            <LuExternalLink
              aria-hidden
              data-testid="plugin-action-external-indicator"
              style={{ height: "9px", width: "9px" }}
            />
          </Box>
        </a>
      </IconButton>
    );
  }

  return (
    <IconButton asChild label={action.name} {...buttonProps}>
      <ReactRouterLink to={internalTo}>
        <PluginActionIcon action={action} />
      </ReactRouterLink>
    </IconButton>
  );
};
