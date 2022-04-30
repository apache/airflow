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

/* global stateColors */

import {
  Box,
  Flex,
  Text,
} from '@chakra-ui/react';
import React from 'react';

const LegendRow = () => (
  <Flex mt={0} mb={2} p={4} flexWrap="wrap">
    {
      Object.entries(stateColors).map(([state, stateColor]) => (
        <Flex alignItems="center" mr={3} key={stateColor}>
          <Box h="12px" w="12px" mr={1} style={{ backgroundColor: stateColor }} />
          <Text fontSize="md">{state}</Text>
        </Flex>
      ))
    }
  </Flex>
);

export default LegendRow;
