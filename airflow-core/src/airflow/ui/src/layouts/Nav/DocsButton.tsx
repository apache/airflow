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
import { FiBookOpen } from "react-icons/fi";

import { Menu } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { NavButton } from "./NavButton";

const baseUrl = document.querySelector("base")?.href ?? "http://localhost:8080/";

const links = [
  {
    href: "https://airflow.apache.org/docs/",
    title: "Documentation",
  },
  {
    href: "https://github.com/apache/airflow",
    title: "GitHub Repo",
  },
  {
    href: new URL("docs", baseUrl).href,
    title: "REST API Reference",
  },
];

export const DocsButton = ({
  showAPI,
  version,
}: {
  readonly showAPI?: boolean;
  readonly version?: string;
}) => {
  const showAPIDocs = Boolean(useConfig("enable_swagger_ui")) && showAPI;

  const versionLink = `https://airflow.apache.org/docs/apache-airflow/${version}/index.html`;

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={<FiBookOpen size="1.75rem" />} title="Docs" />
      </Menu.Trigger>
      <Menu.Content>
        {links
          .filter((link) => !(!showAPIDocs && link.href === "/docs"))
          .map((link) => (
            <Menu.Item asChild key={link.title} value={link.title}>
              <Link aria-label={link.title} href={link.href} rel="noopener noreferrer" target="_blank">
                {link.title}
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
