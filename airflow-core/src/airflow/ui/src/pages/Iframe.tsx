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
import { useParams } from "react-router-dom";

import { usePluginServiceGetPlugins } from "openapi/queries";
import { ProgressBar } from "src/components/ui";

import { ErrorPage } from "./Error";

export const Iframe = () => {
  const { page } = useParams();
  const { data: pluginData, isLoading } = usePluginServiceGetPlugins();

  const iframeView = pluginData?.plugins
    .flatMap((plugin) => plugin.iframe_views)
    .find((view) => (view.url_route ?? view.name.toLowerCase().replace(" ", "-")) === page);

  if (!iframeView) {
    if (isLoading) {
      return (
        <Box flexGrow={1}>
          <ProgressBar />
        </Box>
      );
    }

    return <ErrorPage />;
  }

  return (
    <Box flexGrow={1} m={-3}>
      <iframe
        sandbox="allow-same-origin allow-forms"
        src={iframeView.src}
        style={{ height: "100%", width: "100%" }}
        title={iframeView.name}
      />
    </Box>
  );
};
