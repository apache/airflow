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

export const Iframe = ({ sandbox = "allow-same-origin allow-forms" }: { readonly sandbox: string }) => {
  const { t: translate } = useTranslation();
  const { dagId, mapIndex, page, runId, taskId } = useParams();
  const { data: pluginData, isLoading } = usePluginServiceGetPlugins();

  const iframeView =
    page === "legacy-fab-views"
      ? { href: "/pluginsv2/", name: translate("nav.legacyFabViews") }
      : pluginData?.plugins
          .flatMap((plugin) => plugin.external_views)
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

  // Build the href URL with context parameters if the view has a destination
  let src = iframeView.href;

  if (iframeView.destination !== undefined && iframeView.destination !== "nav") {
    // Check if the href contains placeholders that need to be replaced
    if (dagId !== undefined) {
      src = src.replaceAll("{DAG_ID}", dagId);
    }
    if (runId !== undefined) {
      src = src.replaceAll("{RUN_ID}", runId);
    }
    if (taskId !== undefined) {
      src = src.replaceAll("{TASK_ID}", taskId);
    }
    if (mapIndex !== undefined) {
      src = src.replaceAll("{MAP_INDEX}", mapIndex);
    }
  }

  return (
    <Box
      flexGrow={1}
      height="100%"
      m={-2} // Compensate for parent padding
      minHeight={0}
    >
      <iframe
        sandbox={sandbox}
        src={src}
        style={{
          border: "none",
          display: "block",
          height: "100%",
          width: "100%",
        }}
        title={iframeView.name}
      />
    </Box>
  );
};
