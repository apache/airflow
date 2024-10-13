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
  Menu,
  MenuButton,
  useColorMode,
  MenuItem,
  MenuList,
} from "@chakra-ui/react";
import { FiMoon, FiSun, FiUser } from "react-icons/fi";

import { navButtonProps } from "./navButtonProps";

export const UserSettingsButton = () => {
  const { colorMode, toggleColorMode } = useColorMode();

  return (
    <Menu placement="right">
      <MenuButton
        as={IconButton}
        icon={<FiUser size="1.75rem" />}
        {...navButtonProps}
      />
      <MenuList>
        <MenuItem onClick={toggleColorMode}>
          {colorMode === "light" ? (
            <>
              <FiMoon size="1.25rem" style={{ marginRight: "8px" }} />
              Switch to Dark Mode
            </>
          ) : (
            <>
              <FiSun size="1.25rem" style={{ marginRight: "8px" }} />
              Switch to Light Mode
            </>
          )}
        </MenuItem>
      </MenuList>
    </Menu>
  );
};
