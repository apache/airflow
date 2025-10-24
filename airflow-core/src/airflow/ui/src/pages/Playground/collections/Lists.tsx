/* eslint-disable i18next/no-literal-string */

/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
import { Badge, Box, Heading, HStack, List, Text, VStack } from "@chakra-ui/react";

export const Lists = () => (
  <VStack align="stretch" gap={4}>
    <Heading size="lg">Lists</Heading>
    <VStack align="stretch" gap={4}>
      <Box>
        <Text fontSize="sm" fontWeight="semibold">
          Basic List
        </Text>
        <List.Root variant="plain">
          <List.Item>First item</List.Item>
          <List.Item>Second item</List.Item>
          <List.Item>Third item</List.Item>
          <List.Item>Fourth item</List.Item>
        </List.Root>
      </Box>

      <Box>
        <Text fontSize="sm" fontWeight="semibold">
          List with Icons
        </Text>
        <List.Root variant="plain">
          <List.Item>
            <HStack gap={2}>
              <span>📊</span>
              <Text>Dashboard</Text>
            </HStack>
          </List.Item>
          <List.Item>
            <HStack gap={2}>
              <span>⚙️</span>
              <Text>Settings</Text>
            </HStack>
          </List.Item>
          <List.Item>
            <HStack gap={2}>
              <span>📈</span>
              <Text>Analytics</Text>
            </HStack>
          </List.Item>
          <List.Item>
            <HStack gap={2}>
              <span>🔧</span>
              <Text>Tools</Text>
            </HStack>
          </List.Item>
        </List.Root>
      </Box>

      <Box>
        <Text fontSize="sm" fontWeight="semibold">
          List with Badges
        </Text>
        <List.Root variant="plain">
          <List.Item>
            <HStack justify="space-between" width="full">
              <Text>Task 1</Text>
              <Badge colorPalette="green" variant="solid">Completed</Badge>
            </HStack>
          </List.Item>
          <List.Item>
            <HStack justify="space-between" width="full">
              <Text>Task 2</Text>
              <Badge colorPalette="yellow" variant="solid">In Progress</Badge>
            </HStack>
          </List.Item>
          <List.Item>
            <HStack justify="space-between" width="full">
              <Text>Task 3</Text>
              <Badge colorPalette="red" variant="solid">Failed</Badge>
            </HStack>
          </List.Item>
          <List.Item>
            <HStack justify="space-between" width="full">
              <Text>Task 4</Text>
              <Badge colorPalette="gray" variant="solid">Pending</Badge>
            </HStack>
          </List.Item>
        </List.Root>
      </Box>
    </VStack>
  </VStack>
);

