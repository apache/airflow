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
  Flex, Box, IconButton, Text,
} from '@chakra-ui/react';
import {
  MdOutlineZoomOutMap, MdFilterCenterFocus, MdOutlineAccountTree,
} from 'react-icons/md';
import { HiDatabase } from 'react-icons/hi';

interface Props {
  zoom: any;
  center: () => void;
}

const Legend = ({ zoom, center }: Props) => (
  <Flex justifyContent="space-between" alignItems="center">
    <Box>
      <IconButton
        onClick={zoom.reset}
        fontSize="2xl"
        m={2}
        title="Reset zoom"
        aria-label="Reset zoom"
        icon={<MdOutlineZoomOutMap />}
      />
      <IconButton
        onClick={center}
        fontSize="2xl"
        m={2}
        title="Center"
        aria-label="Center"
        icon={<MdFilterCenterFocus />}
      />
    </Box>
    <Box
      backgroundColor="white"
      p={2}
      borderColor="gray.200"
      borderLeftWidth={1}
      borderTopWidth={1}
    >
      <Text>Legend</Text>
      <Flex>
        <Flex mr={2} alignItems="center">
          <MdOutlineAccountTree size="16px" />
          <Text ml={1}>DAG</Text>
        </Flex>
        <Flex alignItems="center">
          <HiDatabase size="16px" />
          <Text ml={1}>Dataset</Text>
        </Flex>
      </Flex>
    </Box>
  </Flex>
);

export default Legend;
