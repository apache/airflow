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

import React from 'react';
import { Link } from 'react-router-dom';
import {
  Avatar,
  Flex,
  Icon,
  Menu,
  MenuButton,
  MenuDivider,
  MenuList,
  MenuItem,
  useColorMode,
  useColorModeValue,
} from '@chakra-ui/react';
import {
  MdWbSunny,
  MdBrightness2,
  MdAccountCircle,
  MdExitToApp,
  MdQueryBuilder,
} from 'react-icons/md';

import { useAuthContext } from 'providers/auth/context';
import { useDateContext, HOURS_24 } from 'providers/DateProvider';

import ApacheAirflowLogo from 'components/icons/ApacheAirflowLogo';
import TimezoneDropdown from './TimezoneDropdown';

interface Props {
  bodyBg: string;
  overlayBg: string;
  breadcrumb?: React.ReactNode;
}

const AppHeader: React.FC<Props> = ({ bodyBg, overlayBg, breadcrumb }) => {
  const { toggleColorMode } = useColorMode();
  const { dateFormat, toggle24Hour } = useDateContext();
  const headerHeight = '56px';
  const { hasValidAuthToken, logout } = useAuthContext();
  const darkLightIcon = useColorModeValue(MdBrightness2, MdWbSunny);
  const darkLightText = useColorModeValue(' Dark ', ' Light ');

  const handleOpenProfile = () => window.alert('This will take you to your user profile view.');

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
          <TimezoneDropdown />
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
              {/* Clock config should move to User Profile Settings when that page exists */}
              <MenuItem onClick={toggle24Hour}>
                <Icon as={MdQueryBuilder} mr="2" />
                Use
                {dateFormat === HOURS_24 ? ' 12 hour ' : ' 24 hour '}
                clock
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
