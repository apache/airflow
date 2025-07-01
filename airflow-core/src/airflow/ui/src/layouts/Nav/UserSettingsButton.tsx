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
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  FiClock,
  FiGrid,
  FiLogOut,
  FiMoon,
  FiSun,
  FiUser,
  FiGlobe,
  FiMonitor,
  FiEye,
  FiChevronRight,
} from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";
import { useLocalStorage } from "usehooks-ts";

import { Menu } from "src/components/ui";
import { COLOR_MODES, type ColorMode, useColorMode } from "src/context/colorMode/useColorMode";
import { useTimezone } from "src/context/timezone";

import LanguageModal from "./LanguageModal";
import LogoutModal from "./LogoutModal";
import { NavButton } from "./NavButton";
import TimezoneModal from "./TimezoneModal";

dayjs.extend(utc);
dayjs.extend(timezone);

export const UserSettingsButton = () => {
  const { t: translate } = useTranslation();
  const { setColorMode, theme } = useColorMode();
  const { onClose: onCloseTimezone, onOpen: onOpenTimezone, open: isOpenTimezone } = useDisclosure();
  const { onClose: onCloseLogout, onOpen: onOpenLogout, open: isOpenLogout } = useDisclosure();
  const { onClose: onCloseLanguage, onOpen: onOpenLanguage, open: isOpenLanguage } = useDisclosure();
  const { selectedTimezone } = useTimezone();
  const [dagView, setDagView] = useLocalStorage<"graph" | "grid">("default_dag_view", "grid");

  const [time, setTime] = useState(dayjs());

  useEffect(() => {
    const updateTime = () => {
      setTime(dayjs());
    };

    updateTime();

    const interval = setInterval(updateTime, 1000);

    return () => clearInterval(interval);
  }, [selectedTimezone]);

  return (
    <Menu.Root positioning={{ placement: "right" }}>
      <Menu.Trigger asChild>
        <NavButton icon={<FiUser size="1.75rem" />} title={translate("user")} />
      </Menu.Trigger>
      <Menu.Content>
        <Menu.Item onClick={onOpenLanguage} value="language">
          <FiGlobe size="1.25rem" style={{ marginRight: "8px" }} />
          {translate("selectLanguage")}
        </Menu.Item>

        <Menu.Root>
          <Menu.TriggerItem>
            <FiEye size="1.25rem" style={{ marginRight: "8px" }} />
            {translate("appearance.appearance")}
            <FiChevronRight size="1.25rem" style={{ marginLeft: "auto" }} />
          </Menu.TriggerItem>
          <Menu.Content>
            <Menu.RadioItemGroup
              onValueChange={(element) => setColorMode(element.value as ColorMode)}
              value={theme}
            >
              <Menu.RadioItem value={COLOR_MODES.DARK}>
                <FiMoon size="1.25rem" style={{ marginRight: "8px" }} />
                {translate("appearance.darkMode")}
                <Menu.ItemIndicator />
              </Menu.RadioItem>
              <Menu.RadioItem value={COLOR_MODES.LIGHT}>
                <FiSun size="1.25rem" style={{ marginRight: "8px" }} />
                {translate("appearance.lightMode")}
                <Menu.ItemIndicator />
              </Menu.RadioItem>
              <Menu.RadioItem value={COLOR_MODES.SYSTEM}>
                <FiMonitor size="1.25rem" style={{ marginRight: "8px" }} />
                {translate("appearance.systemMode")}
                <Menu.ItemIndicator />
              </Menu.RadioItem>
            </Menu.RadioItemGroup>
          </Menu.Content>
        </Menu.Root>
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
      <LanguageModal isOpen={isOpenLanguage} onClose={onCloseLanguage} />
      <TimezoneModal isOpen={isOpenTimezone} onClose={onCloseTimezone} />
      <LogoutModal isOpen={isOpenLogout} onClose={onCloseLogout} />
    </Menu.Root>
  );
};
