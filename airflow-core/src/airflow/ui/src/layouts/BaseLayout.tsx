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
import { Box, LocaleProvider } from "@chakra-ui/react";
import { useEffect, type PropsWithChildren } from "react";
import { useTranslation } from "react-i18next";
import { Outlet } from "react-router-dom";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { ReactAppResponse } from "openapi/requests/types.gen";
import { ReactPlugin } from "src/pages/ReactPlugin";
import { useConfig } from "src/queries/useConfig";

import { Nav } from "./Nav";

export const BaseLayout = ({ children }: PropsWithChildren) => {
  const instanceName = useConfig("instance_name");
  const { i18n } = useTranslation();
  const { data: pluginData } = usePluginServiceGetPlugins();

  const baseReactPlugins =
    pluginData?.plugins
      .flatMap((plugin) => plugin.react_apps)
      .filter((reactApp: ReactAppResponse) => reactApp.destination === "base") ?? [];

  if (typeof instanceName === "string") {
    document.title = instanceName;
  }

  useEffect(() => {
    const html = document.documentElement;

    const updateHtml = (language: string) => {
      if (language) {
        html.setAttribute("dir", i18n.dir(language));
        html.setAttribute("lang", language);
      }
    };

    i18n.on("languageChanged", updateHtml);

    return () => {
      i18n.off("languageChanged", updateHtml);
    };
  }, [i18n]);

  return (
    <LocaleProvider locale={i18n.language || "en"}>
      <Box display="flex" flexDirection="column" h="100vh">
        <Nav />
        <Box
          _ltr={{ ml: 16 }}
          _rtl={{ mr: 16 }}
          data-testid="main-content"
          display="flex"
          flex={1}
          flexDirection="column"
          minH={0}
          overflowY="auto"
          p={3}
        >
          {baseReactPlugins.map((plugin) => (
            <ReactPlugin key={plugin.name} reactApp={plugin} />
          ))}
          {children ?? <Outlet />}
        </Box>
      </Box>
    </LocaleProvider>
  );
};
