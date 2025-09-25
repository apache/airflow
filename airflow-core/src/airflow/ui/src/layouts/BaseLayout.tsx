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

import { useConfig } from "src/queries/useConfig";

import { Nav } from "./Nav";

export const BaseLayout = ({ children }: PropsWithChildren) => {
  const instanceName = useConfig("instance_name");
  const { i18n } = useTranslation();

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
      <Nav />
      <Box _ltr={{ ml: 20 }} _rtl={{ mr: 20 }} display="flex" flexDirection="column" h="100vh" p={3}>
        {children ?? <Outlet />}
      </Box>
    </LocaleProvider>
  );
};
