/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

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
import { Avatar, Badge, Box, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";

import { Tag } from "src/components/ui";
import { Lists } from "./Lists";

type CollectionsProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Collections = ({ isOpen, onToggle }: CollectionsProps) => {
  const colorPalettes = ["brand", "gray", "red", "green", "blue", "yellow", "purple"];

  return (
    <Box id="collections">
      <Collapsible.Root onOpenChange={onToggle} open={isOpen}>
        <Collapsible.Trigger
          _hover={{ bg: "bg.subtle" }}
          borderColor={isOpen ? "brand.emphasized" : "border.muted"}
          borderWidth="1px"
          cursor="pointer"
          paddingX="6"
          paddingY="4"
          transition="all 0.2s"
          width="full"
        >
          <HStack justify="space-between" width="full">
            <VStack align="flex-start" gap="1">
              <Heading size="xl">Collections</Heading>
              <Text color="fg.muted" fontSize="sm">
                Badges, avatars, tags, and other collection components
              </Text>
            </VStack>
            <Text color="brand.solid" fontSize="lg">
              {isOpen ? "−" : "+"}
            </Text>
          </HStack>
        </Collapsible.Trigger>
        <Collapsible.Content>
          <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
            <VStack align="stretch" gap={6}>
              {/* Badges */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Badges</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Solid Badges
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      {colorPalettes.map((palette) => (
                        <Badge colorPalette={palette} key={`solid-${palette}`} variant="solid">
                          {palette}
                        </Badge>
                      ))}
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Outline Badges
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      {colorPalettes.map((palette) => (
                        <Badge colorPalette={palette} key={`outline-${palette}`} variant="outline">
                          {palette}
                        </Badge>
                      ))}
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Subtle Badges
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      {colorPalettes.map((palette) => (
                        <Badge colorPalette={palette} key={`subtle-${palette}`} variant="subtle">
                          {palette}
                        </Badge>
                      ))}
                    </HStack>
                  </Box>
                </VStack>
              </VStack>

              {/* Avatars */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Avatars</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Avatar Sizes
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      <Avatar.Root size="2xs">
                        <Avatar.Fallback name="John Doe" />
                      </Avatar.Root>
                      <Avatar.Root size="xs">
                        <Avatar.Fallback name="Jane Smith" />
                      </Avatar.Root>
                      <Avatar.Root size="sm">
                        <Avatar.Fallback name="Bob Johnson" />
                      </Avatar.Root>
                      <Avatar.Root size="md">
                        <Avatar.Fallback name="Alice Brown" />
                      </Avatar.Root>
                      <Avatar.Root size="lg">
                        <Avatar.Fallback name="Charlie Wilson" />
                      </Avatar.Root>
                      <Avatar.Root size="xl">
                        <Avatar.Fallback name="Diana Davis" />
                      </Avatar.Root>
                      <Avatar.Root size="2xl">
                        <Avatar.Fallback name="Eve Miller" />
                      </Avatar.Root>
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Avatar with Images
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      <Avatar.Root>
                        <Avatar.Fallback name="User 1" />
                        <Avatar.Fallback>AK</Avatar.Fallback>
                      </Avatar.Root>
                      <Avatar.Root>
                        <Avatar.Fallback name="User 2" />
                        <Avatar.Fallback>AK</Avatar.Fallback>
                      </Avatar.Root>
                      <Avatar.Root>
                        <Avatar.Fallback name="No Image User" />
                      </Avatar.Root>
                    </HStack>
                  </Box>
                </VStack>
              </VStack>

              {/* Tags */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Tags</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Basic Tags
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      <Tag colorPalette="blue">Python</Tag>
                      <Tag colorPalette="green">Data Pipeline</Tag>
                      <Tag colorPalette="orange">ETL</Tag>
                      <Tag colorPalette="purple">Analytics</Tag>
                      <Tag colorPalette="red">Production</Tag>
                      <Tag colorPalette="gray">Development</Tag>
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Closable Tags
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      <Tag closable colorPalette="red" onClose={() => console.log("Removed")}>
                        Removable Tag
                      </Tag>
                      <Tag closable colorPalette="gray" onClose={() => console.log("Removed")}>
                        Another Tag
                      </Tag>
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Tags with Icons (like DAG tags)
                    </Text>
                    <HStack gap={3} wrap="wrap">
                      <Tag colorPalette="blue" startElement={<span>📊</span>}>
                        Dashboard
                      </Tag>
                      <Tag colorPalette="green" startElement={<span>⚡</span>}>
                        Fast
                      </Tag>
                      <Tag colorPalette="orange" startElement={<span>🔒</span>}>
                        Secure
                      </Tag>
                    </HStack>
                  </Box>
                </VStack>
              </VStack>
            </VStack>

            {/* Lists */}
            <Lists />
          </Box>
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};
