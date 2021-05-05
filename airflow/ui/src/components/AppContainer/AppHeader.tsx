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

import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import {
  Avatar,
  Box,
  Button,
  Flex,
  Icon,
  Menu,
  MenuButton,
  MenuDivider,
  MenuList,
  MenuItem,
  useColorMode,
  useColorModeValue,
  Tooltip,
} from '@chakra-ui/react';
import {
  MdWbSunny,
  MdBrightness2,
  MdAccountCircle,
  MdExitToApp,
} from 'react-icons/md';
import dayjs from 'dayjs';
import tz from 'dayjs/plugin/timezone';
import Select from 'components/Select';

import { useAuthContext } from 'providers/auth/context';

import ApacheAirflowLogo from 'components/icons/ApacheAirflowLogo';
import timezones from 'utils/timezones.json';
import { useTimezoneContext } from 'providers/TimezoneProvider';

dayjs.extend(tz);

interface Props {
  bodyBg: string;
  overlayBg: string;
  breadcrumb?: React.ReactNode;
}

interface Option { value: string, label: string }

const AppHeader: React.FC<Props> = ({ bodyBg, overlayBg, breadcrumb }) => {
  const { toggleColorMode } = useColorMode();
  const now = dayjs().tz();
  const headerHeight = '56px';
  const { hasValidAuthToken, logout } = useAuthContext();
  const darkLightIcon = useColorModeValue(MdBrightness2, MdWbSunny);
  const darkLightText = useColorModeValue(' Dark ', ' Light ');
  const { timezone, setTimezone } = useTimezoneContext();
  const [currentTimezone, setCurrentTimezone] = useState<Option | null>();

  const handleOpenProfile = () => window.alert('This will take you to your user profile view.');

  const options = timezones.map(({ group, zones }) => ({
    label: group,
    options: zones.map(({ value, name }) => {
      if (value === timezone && !currentTimezone) setCurrentTimezone({ value, label: name });
      return { value, label: name };
    }),
  }));

  const onChangeTimezone = (newTimezone: Option | null) => {
    setCurrentTimezone(newTimezone);
    if (newTimezone) setTimezone(newTimezone.value);
  };

  return (
    <Flex
      as="header"
      role="banner"
      position="fixed"
      width={`calc(100vw - ${headerHeight})`}
      height={headerHeight}
      zIndex={2}
      align="center"
      justifyContent="space-between"
      py="2"
      px="4"
      backgroundColor={overlayBg}
      borderBottomWidth="1px"
      borderBottomColor={bodyBg}
    >
      {breadcrumb}
      {!breadcrumb && (
        <Link to="/" aria-label="Back to home">
          <ApacheAirflowLogo />
        </Link>
      )}
      {hasValidAuthToken && (
        <Flex align="center">
          <Menu closeOnSelect={false} autoSelect={false}>
            <Tooltip label="Change time zone" hasArrow>
              <MenuButton as={Button} variant="ghost" mr="4">
                <Box
                  as="time"
                  dateTime={now.toString()}
                  fontSize="md"
                >
                  {now.format('h:mmA Z')}
                </Box>
              </MenuButton>
            </Tooltip>
            <MenuList placement="top-end">
              <Box px="3" pb="1">
                <Select
                  options={options}
                  value={currentTimezone}
                  onChange={onChangeTimezone}
                />
              </Box>
            </MenuList>
          </Menu>
          <Menu>
            <MenuButton>
              <Avatar name="Ryan Hamilton" size="sm" color="blue.900" bg="blue.200" />
            </MenuButton>
            <MenuList placement="top-end">
              <MenuItem onClick={handleOpenProfile}>
                <Icon as={MdAccountCircle} mr="2" />
                Your Profile
              </MenuItem>
              <MenuItem onClick={toggleColorMode}>
                <Icon as={darkLightIcon} mr="2" />
                Set
                {darkLightText}
                Mode
              </MenuItem>
              <MenuDivider />
              <MenuItem onClick={logout}>
                <Icon as={MdExitToApp} mr="2" />
                Logout
              </MenuItem>
            </MenuList>
          </Menu>
        </Flex>
      )}
    </Flex>
  );
};

export default AppHeader;
