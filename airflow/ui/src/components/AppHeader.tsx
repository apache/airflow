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
import {
  Button,
  Flex,
  Box,
  Icon,
} from '@chakra-ui/react';
import { MdExitToApp } from 'react-icons/md';
import { useAuthContext } from 'auth/context';

const AppHeader: React.FC = ({ children }) => {
  const { logout } = useAuthContext();

  return (
    <Flex width="100vw" height="100vh">
      <Flex
        as="header"
        position="fixed"
        width="100vw"
        zIndex={2}
        align="center"
        justifyContent="space-between"
        py="2"
        px="4"
        borderBottomWidth="1px"
      >
        <Box />
        <Button onClick={logout}>
          <Icon as={MdExitToApp} mr="2" />
          Logout
        </Button>
      </Flex>
      {children}
    </Flex>
  );
};

export default AppHeader;
