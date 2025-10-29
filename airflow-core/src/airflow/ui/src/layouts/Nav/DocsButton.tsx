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
import { Box, Icon, Link } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiBookOpen, FiExternalLink } from "react-icons/fi";

import { Menu } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";
import type { NavItemResponse } from "src/utils/types";

import { NavButton } from "./NavButton";
import { PluginMenuItem } from "./PluginMenuItem";

const baseUrl = document.querySelector("base")?.href ?? "http://localhost:8080/";

const links = [
  {
    href: "https://airflow.apache.org/docs/",
    key: "documentation",
  },
  {
    href: "https://github.com/apache/airflow",
    key: "githubRepo",
  },
  {
    href: new URL("docs", baseUrl).href,
    key: "restApiReference",
  },
];

export const DocsButton = ({
  externalViews,
  showAPI,
  version,
}: {
  readonly externalViews: Array<NavItemResponse>;
  readonly showAPI?: boolean;
  readonly version?: string;
}) => {
  const { t: translate } = useTranslation("common");
  const showAPIDocs = Boolean(useConfig("enable_swagger_ui")) && showAPI;

  const versionLink = `https://airflow.apache.org/docs/apache-airflow/${version}/index.html`;

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={FiBookOpen} title={translate("nav.docs")} />
      </Menu.Trigger>
      <Menu.Content>
        {links
          .filter((link) => !(!showAPIDocs && link.href === "/docs"))
          .map((link) => (
            <Menu.Item asChild key={link.key} value={translate(`docs.${link.key}`)}>
              <Link
                aria-label={translate(`docs.${link.key}`)}
                href={link.href}
                rel="noopener noreferrer"
                target="_blank"
                textDecoration="none"
              >
                <Box flex="1">{translate(`docs.${link.key}`)}</Box>
                <Icon as={FiExternalLink} boxSize={4} color="fg.muted" />
              </Link>
            </Menu.Item>
          ))}
        {version === undefined ? undefined : (
          <Menu.Item asChild key={version} value={version}>
            <Link aria-label={version} href={versionLink} rel="noopener noreferrer" target="_blank">
              <Box flex="1">{version}</Box>
              <Icon as={FiExternalLink} boxSize={4} color="fg.muted" />
            </Link>
          </Menu.Item>
        )}
        {externalViews.map((view) => (
          <PluginMenuItem {...view} key={view.name} />
        ))}
      </Menu.Content>
    </Menu.Root>
  );
};
