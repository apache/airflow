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

export const DocsButton = () => (
  <Menu placement="right">
    <MenuButton
      as={IconButton}
      icon={<FiBookOpen size="1.75rem" />}
      {...navButtonProps}
    />
    <MenuList>
      <MenuItem
        as={Link}
        href="https://airflow.apache.org/docs/"
        target="_blank"
      >
        Documentation
      </MenuItem>
      <MenuItem
        as={Link}
        href="https://github.com/apache/airflow"
        target="_blank"
      >
        Github Repo
      </MenuItem>
      <MenuItem
        as={Link}
        href={`${import.meta.env.VITE_FASTAPI_URL}/docs`}
        target="_blank"
      >
        REST API Reference
      </MenuItem>
    </MenuList>
  </Menu>
);
