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
import { Box, Icon, useDisclosure } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import {
  FiGrid,
  FiLogOut,
  FiMoon,
  FiSun,
  FiUser,
  FiGlobe,
  FiEye,
  FiChevronRight,
  FiChevronLeft,
  FiMonitor,
} from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";
import { useLocalStorage } from "usehooks-ts";

import { Menu } from "src/components/ui";
import { useColorMode } from "src/context/colorMode/useColorMode";
import type { NavItemResponse } from "src/utils/types";

import LanguageModal from "./LanguageModal";
import LogoutModal from "./LogoutModal";
import { NavButton } from "./NavButton";
import { PluginMenuItem } from "./PluginMenuItem";
import { TimezoneMenuItem } from "./TimezoneMenuItem";
import TimezoneModal from "./TimezoneModal";

const COLOR_MODES = {
  DARK: "dark",
  LIGHT: "light",
  SYSTEM: "system",
} as const;

type ColorMode = (typeof COLOR_MODES)[keyof typeof COLOR_MODES];

export const UserSettingsButton = ({ externalViews }: { readonly externalViews: Array<NavItemResponse> }) => {
  const { i18n, t: translate } = useTranslation();
  const { selectedTheme, setColorMode } = useColorMode();

  const colorModeOptions = [
    {
      icon: FiSun,
      label: translate("appearance.lightMode"),
      value: COLOR_MODES.LIGHT,
    },
    {
      icon: FiMoon,
      label: translate("appearance.darkMode"),
      value: COLOR_MODES.DARK,
    },
    {
      icon: FiMonitor,
      label: translate("appearance.systemMode"),
      value: COLOR_MODES.SYSTEM,
    },
  ];

  const { onClose: onCloseTimezone, onOpen: onOpenTimezone, open: isOpenTimezone } = useDisclosure();
  const { onClose: onCloseLogout, onOpen: onOpenLogout, open: isOpenLogout } = useDisclosure();
  const { onClose: onCloseLanguage, onOpen: onOpenLanguage, open: isOpenLanguage } = useDisclosure();

  const [dagView, setDagView] = useLocalStorage<"graph" | "grid">("default_dag_view", "grid");

  const theme = selectedTheme ?? COLOR_MODES.SYSTEM;

  const isRTL = i18n.dir() === "rtl";

  return (
    <>
      <Menu.Root positioning={{ placement: "right" }}>
        <Menu.Trigger asChild>
          <NavButton icon={FiUser} title={translate("user")} />
        </Menu.Trigger>
        <Menu.Content>
          <Menu.Item onClick={onOpenLanguage} value="language">
            <Icon as={FiGlobe} boxSize={4} />
            <Box flex="1">{translate("selectLanguage")}</Box>
          </Menu.Item>
          <Menu.Root>
            <Menu.TriggerItem>
              <Icon as={FiEye} boxSize={4} />
              <Box flex="1">{translate("appearance.appearance")}</Box>
              <Icon as={isRTL ? FiChevronLeft : FiChevronRight} boxSize={4} color="fg.muted" />
            </Menu.TriggerItem>
            <Menu.Content>
              <Menu.RadioItemGroup
                onValueChange={(element) => setColorMode(element.value as ColorMode)}
                value={theme}
              >
                {colorModeOptions.map(({ icon, label, value }) => (
                  <Menu.RadioItem key={value} value={value}>
                    <Icon as={icon} boxSize={4} />
                    <Box flex="1">{label}</Box>
                    <Menu.ItemIndicator color="fg.muted" />
                  </Menu.RadioItem>
                ))}
              </Menu.RadioItemGroup>
            </Menu.Content>
          </Menu.Root>
          <Menu.Item
            onClick={() => (dagView === "grid" ? setDagView("graph") : setDagView("grid"))}
            value={dagView}
          >
            <Icon as={dagView === "grid" ? MdOutlineAccountTree : FiGrid} boxSize={4} />
            <Box flex="1">
              {dagView === "grid" ? translate("defaultToGraphView") : translate("defaultToGridView")}
            </Box>
          </Menu.Item>
          <TimezoneMenuItem onOpen={onOpenTimezone} />
          {externalViews.map((view) => (
            <PluginMenuItem {...view} key={view.name} />
          ))}
          <Menu.Separator />
          <Menu.Item onClick={onOpenLogout} value="logout">
            <Icon as={FiLogOut} boxSize={4} transform={isRTL ? "rotate(180deg)" : undefined} />
            <Box flex="1">{translate("logout")}</Box>
          </Menu.Item>
        </Menu.Content>
      </Menu.Root>
      <LanguageModal isOpen={isOpenLanguage} onClose={onCloseLanguage} />
      <TimezoneModal isOpen={isOpenTimezone} onClose={onCloseTimezone} />
      <LogoutModal isOpen={isOpenLogout} onClose={onCloseLogout} />
    </>
  );
};
