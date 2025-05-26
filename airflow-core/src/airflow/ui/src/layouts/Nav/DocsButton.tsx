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
import { Link } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiBookOpen } from "react-icons/fi";

import { Menu } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { NavButton } from "./NavButton";

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
  showAPI,
  version,
}: {
  readonly showAPI?: boolean;
  readonly version?: string;
}) => {
  const { t: translate } = useTranslation("common");
  const showAPIDocs = Boolean(useConfig("enable_swagger_ui")) && showAPI;

  const versionLink = `https://airflow.apache.org/docs/apache-airflow/${version}/index.html`;

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={<FiBookOpen size="1.75rem" />} title={translate("nav.docs")} />
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
              >
                {translate(`docs.${link.key}`)}
              </Link>
            </Menu.Item>
          ))}
        {version === undefined ? undefined : (
          <Menu.Item asChild key={version} value={version}>
            <Link aria-label={version} href={versionLink} rel="noopener noreferrer" target="_blank">
              {version}
            </Link>
          </Menu.Item>
        )}
      </Menu.Content>
    </Menu.Root>
  );
};
