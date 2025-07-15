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
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { usePluginServiceGetPlugins } from "openapi/queries";
import { ProgressBar } from "src/components/ui";

import { ErrorPage } from "./Error";
import { Iframe } from "./Iframe";
import { ReactPlugin } from "./ReactPlugin";

export const PluginView = () => {
  const { t: translate } = useTranslation();
  const { page } = useParams();
  const { data: pluginData, isLoading } = usePluginServiceGetPlugins();

  // Check for external_views first
  const externalView =
    page === "legacy-fab-views"
      ? {
          destination: "nav" as const,
          href: "/pluginsv2/",
          name: translate("nav.legacyFabViews"),
          url_route: "legacy-fab-views",
        }
      : pluginData?.plugins
          .flatMap((plugin) => plugin.external_views)
          .find((view) => (view.url_route ?? view.name.toLowerCase().replace(" ", "-")) === page);

  // Check for react_apps if no external view found
  const reactApp = pluginData?.plugins
    .flatMap((plugin) => plugin.react_apps)
    .find((view) => (view.url_route ?? view.name.toLowerCase().replace(" ", "-")) === page);

  if (isLoading) {
    return (
      <Box flexGrow={1}>
        <ProgressBar />
      </Box>
    );
  }

  // If external view is found, render Iframe component
  if (externalView) {
    return (
      <Box
        flexGrow={1}
        height="100%"
        m={-2} // Compensate for parent padding
        minHeight={0}
      >
        <Iframe externalView={externalView} sandbox="allow-scripts allow-same-origin allow-forms" />
      </Box>
    );
  }

  // If react app is found, render ReactPlugin component
  if (reactApp) {
    return (
      <Box
        flexGrow={1}
        height="100%"
        m={-2} // Compensate for parent padding
        minHeight={0}
      >
        <ReactPlugin reactApp={reactApp} />
      </Box>
    );
  }

  // If neither is found, render error page
  return <ErrorPage />;
};
