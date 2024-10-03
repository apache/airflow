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
import {
  IconButton,
  Link,
  Menu,
  MenuButton,
  MenuItem,
  MenuList,
} from "@chakra-ui/react";
import { FiBookOpen } from "react-icons/fi";

import { navButtonProps } from "./navButtonProps";

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
    href: `${import.meta.env.VITE_FASTAPI_URL}/docs`,
    title: "REST API Reference",
  },
];

export const DocsButton = () => (
  <Menu placement="right">
    <MenuButton
      as={IconButton}
      icon={<FiBookOpen size="1.75rem" />}
      {...navButtonProps}
    />
    <MenuList>
      {links.map((link) => (
        <MenuItem
          aria-label={link.title}
          as={Link}
          href={link.href}
          key={link.title}
          target="_blank"
        >
          {link.title}
        </MenuItem>
      ))}
    </MenuList>
  </Menu>
);
