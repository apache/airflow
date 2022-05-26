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
  Flex,
  Text,
  HStack,
} from '@chakra-ui/react';
import React from 'react';

const StatusBadge = ({
  state, stateColor, setHoveredTaskState, displayValue,
}) => (
  <Text
    borderRadius={4}
    border={`solid 2px ${stateColor}`}
    px={1}
    cursor="pointer"
    fontSize="11px"
    onMouseEnter={() => setHoveredTaskState(state)}
    onMouseLeave={() => setHoveredTaskState()}
  >
    {displayValue || state }
  </Text>
);

const LegendRow = ({ setHoveredTaskState }) => (
  <Flex p={4} flexWrap="wrap" justifyContent="end">
    <HStack spacing={2}>
      {
      Object.entries(stateColors).map(([state, stateColor]) => (
        <StatusBadge
          key={state}
          state={state}
          stateColor={stateColor}
          setHoveredTaskState={setHoveredTaskState}
        />
      ))
      }
      <StatusBadge
        key="no_status"
        displayValue="no_status"
        state={null}
        stateColor="white"
        setHoveredTaskState={setHoveredTaskState}
      />
    </HStack>
  </Flex>
);

export default LegendRow;
