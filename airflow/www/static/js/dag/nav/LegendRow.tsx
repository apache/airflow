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

import { Flex, Text, HStack, Center, Kbd } from "@chakra-ui/react";
import React from "react";

interface LegendProps {
  onStatusHover: (status: string | null) => void;
  onStatusLeave: () => void;
}

interface BadgeProps extends LegendProps {
  state: string | null;
  stateColor: string;
  displayValue?: string;
}

const StatusBadge = ({
  state,
  stateColor,
  onStatusHover,
  onStatusLeave,
  displayValue,
}: BadgeProps) => (
  <Text
    borderRadius={4}
    border={`solid 2px ${stateColor}`}
    px={1}
    cursor="pointer"
    fontSize="11px"
    onMouseEnter={() => onStatusHover(state)}
    onMouseLeave={() => onStatusLeave()}
  >
    {displayValue || state}
  </Text>
);

const LegendRow = ({ onStatusHover, onStatusLeave }: LegendProps) => (
  <Flex p={4} justifyContent="space-between" flexWrap="wrap">
    <Center>
      <Text fontSize="11px" position="relative">
        Press{" "}
        <Kbd position="relative" top={-0.5}>
          shift
        </Kbd>{" "}
        +{" "}
        <Kbd position="relative" top={-0.5}>
          /
        </Kbd>{" "}
        for Shortcuts
      </Text>
    </Center>
    <Flex flexWrap="wrap" justifyContent="end">
      <HStack spacing={2} wrap="wrap">
        {Object.entries(stateColors).map(([state, stateColor]) => (
          <StatusBadge
            key={state}
            state={state}
            stateColor={stateColor}
            onStatusHover={onStatusHover}
            onStatusLeave={onStatusLeave}
          />
        ))}
        <StatusBadge
          key="no_status"
          displayValue="no_status"
          state={null}
          stateColor="white"
          onStatusHover={onStatusHover}
          onStatusLeave={onStatusLeave}
        />
      </HStack>
    </Flex>
  </Flex>
);

export default LegendRow;
