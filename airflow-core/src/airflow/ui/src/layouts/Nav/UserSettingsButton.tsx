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
import { Box, HStack, Icon, useDisclosure, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import {
  FiKey,
  FiLogOut,
  FiMoon,
  FiSettings,
  FiSun,
  FiUser,
  FiGlobe,
  FiEye,
  FiChevronRight,
  FiChevronLeft,
  FiMonitor,
} from "react-icons/fi";

import { useAuthLinksServiceGetCurrentUserInfo } from "openapi/queries";
import { useColorMode } from "src/context/colorMode/useColorMode";
import { Menu, Tooltip } from "src/system-components";
import { RouterLink } from "src/system-components/RouterLink";
import type { NavItemResponse } from "src/utils/types";

import LanguageModal from "./LanguageModal";
import LogoutModal from "./LogoutModal";
import { NavButton } from "./NavButton";
import { PluginMenuItem } from "./PluginMenuItem";
import TokenGenerationModal from "./TokenGenerationModal";

// Beyond a handful of teams the list would push the menu items below the fold.
const MAX_VISIBLE_TEAMS = 5;

const COLOR_MODES = {
  DARK: "dark",
  LIGHT: "light",
  SYSTEM: "system",
} as const;

export const UserSettingsButton = ({ externalViews }: { readonly externalViews: Array<NavItemResponse> }) => {
  const { i18n, t: translate } = useTranslation();
  const { selectedTheme, setColorMode } = useColorMode();
  const { data: currentUser } = useAuthLinksServiceGetCurrentUserInfo();

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

  const { onClose: onCloseLogout, onOpen: onOpenLogout, open: isOpenLogout } = useDisclosure();
  const { onClose: onCloseLanguage, onOpen: onOpenLanguage, open: isOpenLanguage } = useDisclosure();
  const { onClose: onCloseToken, onOpen: onOpenToken, open: isOpenToken } = useDisclosure();

  const theme = selectedTheme ?? COLOR_MODES.SYSTEM;

  const isRTL = i18n.dir() === "rtl";

  return (
    <>
      <Menu.Root positioning={{ placement: "right" }}>
        <Menu.Trigger asChild>
          <NavButton icon={FiUser} title={translate("user")} />
        </Menu.Trigger>
        <Menu.Content>
          {currentUser ? (
            <>
              <Box p={3}>
                <Box color="fg.muted" fontSize="sm">
                  {translate("signedInAs")}
                </Box>
                <Box fontSize="md" fontWeight="semibold">
                  {`${currentUser.username} (id: ${currentUser.id})`}
                </Box>
                {Array.isArray(currentUser.teams) ? (
                  <>
                    <Box color="fg.muted" fontSize="sm" mt={2}>
                      {translate("teams.title")}
                    </Box>
                    {currentUser.teams.length ? (
                      <HStack fontSize="sm" gap={2} wrap="wrap">
                        {currentUser.teams.slice(0, MAX_VISIBLE_TEAMS).map((team) => (
                          <Box key={team}>{team}</Box>
                        ))}
                        {currentUser.teams.length > MAX_VISIBLE_TEAMS ? (
                          <Tooltip
                            closeDelay={0}
                            content={
                              <VStack align="start" gap={0}>
                                {currentUser.teams.slice(MAX_VISIBLE_TEAMS).map((team) => (
                                  <Box key={team}>{team}</Box>
                                ))}
                              </VStack>
                            }
                            openDelay={0}
                            portalled
                          >
                            <Box color="fg.muted" textDecoration="underline dotted">
                              {translate("teams.more", {
                                count: currentUser.teams.length - MAX_VISIBLE_TEAMS,
                              })}
                            </Box>
                          </Tooltip>
                        ) : undefined}
                      </HStack>
                    ) : (
                      <Box color="fg.muted" fontSize="sm">
                        {translate("teams.none")}
                      </Box>
                    )}
                  </>
                ) : undefined}
              </Box>
              <Menu.Separator />
            </>
          ) : undefined}
          <Menu.Item asChild value="settings">
            <RouterLink color="inherit" to="/settings">
              <Icon as={FiSettings} boxSize={4} />
              <Box flex="1">{translate("settings.title")}</Box>
            </RouterLink>
          </Menu.Item>
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
              <Menu.RadioItemGroup onValueChange={(element) => setColorMode(element.value)} value={theme}>
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
          <Menu.Item onClick={onOpenToken} value="generateToken">
            <Icon as={FiKey} boxSize={4} />
            <Box flex="1">{translate("generateToken")}</Box>
          </Menu.Item>
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
      <LogoutModal isOpen={isOpenLogout} onClose={onCloseLogout} />
      <TokenGenerationModal isOpen={isOpenToken} onClose={onCloseToken} />
    </>
  );
};
