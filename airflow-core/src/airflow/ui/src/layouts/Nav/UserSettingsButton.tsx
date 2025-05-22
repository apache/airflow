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
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FiClock, FiGrid, FiLogOut, FiMoon, FiSun, FiUser, FiGlobe, FiChevronRight } from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";
import { useLocalStorage } from "usehooks-ts";

import { Menu } from "src/components/ui";
import { useColorMode } from "src/context/colorMode/useColorMode";
import { useTimezone } from "src/context/timezone";
import { supportedLanguages } from "src/i18n/config";

import LogoutModal from "./LogoutModal";
import { NavButton } from "./NavButton";
import TimezoneModal from "./TimezoneModal";

dayjs.extend(utc);
dayjs.extend(timezone);

export const UserSettingsButton = () => {
  const { i18n, t: translate } = useTranslation();
  const { colorMode, toggleColorMode } = useColorMode();
  const { onClose: onCloseTimezone, onOpen: onOpenTimezone, open: isOpenTimezone } = useDisclosure();
  const { onClose: onCloseLogout, onOpen: onOpenLogout, open: isOpenLogout } = useDisclosure();
  const { selectedTimezone } = useTimezone();
  const [dagView, setDagView] = useLocalStorage<"graph" | "grid">("default_dag_view", "grid");

  const [time, setTime] = useState(dayjs());

  return (
    <Menu.Root onOpenChange={() => setTime(dayjs())} positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={<FiUser size="1.75rem" />} title={translate("user")} />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.Root>
          <Menu.TriggerItem>
            <FiGlobe size="1.25rem" style={{ marginRight: "8px" }} />
            {translate("language.select")}
            <FiChevronRight size="1.25rem" style={{ marginLeft: "auto" }} />
          </Menu.TriggerItem>
          <Menu.Content>
            <Menu.RadioItemGroup
              onValueChange={(element) => void i18n.changeLanguage(element.value)}
              value={i18n.language}
            >
              {supportedLanguages.map((lang) => (
                <Menu.RadioItem key={lang.code} value={lang.code}>
                  {lang.name}
                  <Menu.ItemIndicator />
                </Menu.RadioItem>
              ))}
            </Menu.RadioItemGroup>
          </Menu.Content>
        </Menu.Root>
        <Menu.Item onClick={toggleColorMode} value="color-mode">
          {colorMode === "light" ? (
            <>
              <FiMoon size="1.25rem" style={{ marginRight: "8px" }} />
              {translate("switchToDarkMode")}
            </>
          ) : (
            <>
              <FiSun size="1.25rem" style={{ marginRight: "8px" }} />
              {translate("switchToLightMode")}
            </>
          )}
        </Menu.Item>
        <Menu.Item
          onClick={() => (dagView === "grid" ? setDagView("graph") : setDagView("grid"))}
          value={dagView}
        >
          {dagView === "grid" ? (
            <>
              <MdOutlineAccountTree size="1.25rem" style={{ marginRight: "8px" }} />
              {translate("defaultToGraphView")}
            </>
          ) : (
            <>
              <FiGrid size="1.25rem" style={{ marginRight: "8px" }} />
              {translate("defaultToGridView")}
            </>
          )}
        </Menu.Item>
        <Menu.Item onClick={onOpenTimezone} value="timezone">
          <FiClock size="1.25rem" style={{ marginRight: "8px" }} />
          {translate("timezone")}: {dayjs(time).tz(selectedTimezone).format("HH:mm z (Z)")}
        </Menu.Item>
        <Menu.Item onClick={onOpenLogout} value="logout">
          <FiLogOut size="1.25rem" style={{ marginRight: "8px" }} />
          {translate("logout")}
        </Menu.Item>
      </Menu.Content>
      <TimezoneModal isOpen={isOpenTimezone} onClose={onCloseTimezone} />
      <LogoutModal isOpen={isOpenLogout} onClose={onCloseLogout} />
    </Menu.Root>
  );
};
