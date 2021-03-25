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
  Box,
  Heading,
  useColorModeValue,
} from '@chakra-ui/react';

import SectionNavBtn from 'components/SectionNavBtn';

import AppContainer from 'components/AppContainer';

interface Props {
  currentSection: string;
  currentView: string;
  navItems: {
    label: string;
    path: string;
  }[]
  toolBar?: React.ReactNode;
}

const SectionWrapper: React.FC<Props> = ({
  children, currentSection, currentView, navItems, toolBar,
}) => (
  <AppContainer
    breadcrumb={(
      <Heading as="h1" size="md">
        <Box
          as="span"
          color={useColorModeValue('gray.400', 'gray.500')}
        >
          {currentSection}
          /
        </Box>
        {currentView}
      </Heading>
    )}
  >
    <Box
      pt={2}
      mx={-4}
      px={4}
      pb="2"
      bg={useColorModeValue('gray.100', 'gray.700')}
    >
      <Box
        display="flex"
        alignItems="center"
        justifyContent="space-between"
      >
        <Box as="nav">
          {navItems.map((item) => (
            <SectionNavBtn key={item.label} item={item} currentLabel={currentView} />
          ))}
        </Box>
      </Box>
    </Box>
    {toolBar && (
      <Box
        position="sticky"
        top="0"
        zIndex="1"
        display="flex"
        justifyContent="flex-start"
        width="calc(100% + 2rem)%"
        mr={-4}
        ml={-4}
        py={2}
        px={4}
        borderBottomWidth="2px"
        borderBottomColor={useColorModeValue('gray.100', 'gray.700')}
        backgroundColor={useColorModeValue('white', 'gray.800')}
      >
        {toolBar}
      </Box>
    )}
    <Box py="4">{children}</Box>
  </AppContainer>
);

export default SectionWrapper;
