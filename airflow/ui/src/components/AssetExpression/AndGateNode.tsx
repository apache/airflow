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
import { Box, VStack, Badge } from "@chakra-ui/react";
import type { PropsWithChildren } from "react";
import { TbLogicAnd } from "react-icons/tb";

export const AndGateNode = ({ children }: PropsWithChildren) => (
  <Box
    bg="bg.subtle"
    border="2px dashed"
    borderRadius="lg"
    display="inline-block"
    minW="fit-content"
    p={4}
    position="relative"
  >
    <Badge
      alignItems="center"
      borderRadius="full"
      display="flex"
      fontSize="sm"
      gap={1}
      left="50%"
      position="absolute"
      px={3}
      py={1}
      top="-3"
      transform="translateX(-50%)"
    >
      <TbLogicAnd size={18} />
      AND
    </Badge>
    <VStack align="center" gap={4} mt={3}>
      {children}
    </VStack>
  </Box>
);
