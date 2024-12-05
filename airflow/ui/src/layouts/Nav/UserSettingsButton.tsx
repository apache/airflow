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
import { useDisclosure } from "@chakra-ui/react";
import dayjs from "dayjs";
import timezone from "dayjs/plugin/timezone";
import utc from "dayjs/plugin/utc";
import { FiClock, FiMoon, FiSun, FiUser } from "react-icons/fi";

import { useVersionServiceGetVersion } from "openapi/queries";

import { Menu } from "src/components/ui";
import { useColorMode } from "src/context/colorMode/useColorMode";
import { useTimezone } from "src/context/timezone";

import { NavButton } from "./NavButton";
import TimezoneModal from "./TimezoneModal";

dayjs.extend(utc);
dayjs.extend(timezone);

export const UserSettingsButton = () => {
  const { colorMode, toggleColorMode } = useColorMode();
  const { onClose, onOpen, open } = useDisclosure();
  const { selectedTimezone } = useTimezone();
  const { data } = useVersionServiceGetVersion();

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={<FiUser size="1.75rem" />} title="User" />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.Item onClick={toggleColorMode} value="color-mode">
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
        </Menu.Item>
        <Menu.Item onClick={onOpen} value="timezone">
          <FiClock size="1.25rem" style={{ marginRight: "8px" }} />
          {dayjs().tz(selectedTimezone).format("HH:mm z (Z)")}
        </Menu.Item>
        <Menu.Item value="version">
          Version <a href={`https://pypi.org/project/apache-airflow/${data?.version}/`} rel="noreferrer" style={{color: "#365f84"}} target="_blank">{data?.version}</a>
        </Menu.Item>
      </Menu.Content>
      <TimezoneModal isOpen={open} onClose={onClose} />
    </Menu.Root>
  );
};
