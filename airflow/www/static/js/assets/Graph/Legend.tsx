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

import React from "react";
import { Flex, Box, Text } from "@chakra-ui/react";
import { MdOutlineAccountTree } from "react-icons/md";
import { HiDatabase } from "react-icons/hi";
import { PiRectangleDashed } from "react-icons/pi";

const Legend = () => (
  <Box
    backgroundColor="white"
    p={2}
    borderColor="gray.200"
    borderWidth={1}
    fontSize={14}
  >
    <Flex>
      <Flex mr={2} alignItems="center">
        <MdOutlineAccountTree size="14px" />
        <Text ml={1}>DAG</Text>
      </Flex>
      <Flex alignItems="center" mr={2}>
        <HiDatabase size="14px" />
        <Text ml={1}>Asset</Text>
      </Flex>
      <Flex alignItems="center">
        <PiRectangleDashed size="14px" />
        <Text ml={1}>Asset Alias</Text>
      </Flex>
    </Flex>
  </Box>
);

export default Legend;
