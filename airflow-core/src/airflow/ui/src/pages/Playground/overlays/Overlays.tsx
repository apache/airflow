/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */
import { Box, Collapsible, Heading, HStack, Text, VStack, Button, Menu, Popover, Tooltip } from "@chakra-ui/react";
import { useState } from "react";

type OverlaysProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Overlays = ({ isOpen, onToggle }: OverlaysProps) => {
  const [isPopoverOpen, setIsPopoverOpen] = useState(false);

  return (
    <Box id="overlays">
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
              <Heading size="xl">Overlays</Heading>
              <Text color="fg.muted" fontSize="sm">
                Modals, popovers, tooltips, and menu components
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
              {/* Tooltips */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Tooltips</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Basic Tooltips
                    </Text>
                    <HStack gap={4} wrap="wrap">
                      <Tooltip.Root>
                        <Tooltip.Trigger>
                          <Button colorPalette="brand" variant="outline">
                            Hover me
                          </Button>
                        </Tooltip.Trigger>
                        <Tooltip.Content padding={4}>
                          <Tooltip.Arrow />
                          <Text>This is a helpful tooltip</Text>
                        </Tooltip.Content>
                      </Tooltip.Root>

                      <Tooltip.Root>
                        <Tooltip.Trigger>
                          <Button colorPalette="green" variant="outline">
                            Success tooltip
                          </Button>
                        </Tooltip.Trigger>
                        <Tooltip.Content padding={4}>
                          <Tooltip.Arrow />
                          <Text>Operation completed successfully</Text>
                        </Tooltip.Content>
                      </Tooltip.Root>

                      <Tooltip.Root>
                        <Tooltip.Trigger>
                          <Button colorPalette="red" variant="outline">
                            Error tooltip
                          </Button>
                        </Tooltip.Trigger>
                        <Tooltip.Content padding={4}>
                          <Tooltip.Arrow />
                          <Text>Something went wrong</Text>
                        </Tooltip.Content>
                      </Tooltip.Root>
                    </HStack>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Tooltip with Rich Content
                    </Text>
                    <Tooltip.Root>
                      <Tooltip.Trigger>
                        <Button colorPalette="purple" variant="outline">
                          Rich tooltip
                        </Button>
                      </Tooltip.Trigger>
                      <Tooltip.Content padding={4}>
                        <Tooltip.Arrow />
                        <VStack align="flex-start" gap={1}>
                          <Text fontWeight="semibold">Advanced Feature</Text>
                          <Text color="fg.muted" fontSize="sm">
                            This tooltip contains multiple lines of information
                          </Text>
                        </VStack>
                      </Tooltip.Content>
                    </Tooltip.Root>
                  </Box>
                </VStack>
              </VStack>

              {/* Popovers */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Popovers</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Basic Popover
                    </Text>
                    <Popover.Root onOpenChange={setIsPopoverOpen} open={isPopoverOpen}>
                      <Popover.Trigger>
                        <Button colorPalette="blue" variant="outline">
                          Open Popover
                        </Button>
                      </Popover.Trigger>
                      <Popover.Content padding={6}>
                        <Popover.Arrow />
                        <Popover.CloseTrigger />
                        <VStack align="stretch" gap={3}>
                          <Heading size="sm">Popover Title</Heading>
                          <Text color="fg.muted" fontSize="sm">
                            This is a popover with interactive content. You can include forms,
                            buttons, or any other components here.
                          </Text>
                          <HStack gap={2}>
                            <Button colorPalette="brand" size="sm">
                              Action
                            </Button>
                            <Button 
                              colorPalette="gray" 
                              size="sm" 
                              variant="outline"
                              onClick={() => setIsPopoverOpen(false)}
                            >
                              Cancel
                            </Button>
                          </HStack>
                        </VStack>
                      </Popover.Content>
                    </Popover.Root>
                  </Box>
                </VStack>
              </VStack>

              {/* Menus */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Menus</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Basic Menu
                    </Text>
                    <Menu.Root>
                      <Menu.Trigger>
                        <Button colorPalette="gray" variant="outline">
                          Open Menu
                        </Button>
                      </Menu.Trigger>
                      <Menu.Content>
                        <Menu.Item>Profile</Menu.Item>
                        <Menu.Item>Settings</Menu.Item>
                        <Menu.Item>Help</Menu.Item>
                        <Menu.Item>Sign out</Menu.Item>
                      </Menu.Content>
                    </Menu.Root>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Menu with Groups
                    </Text>
                    <Menu.Root>
                      <Menu.Trigger>
                        <Button colorPalette="green" variant="outline">
                          Actions Menu
                        </Button>
                      </Menu.Trigger>
                      <Menu.Content>
                        <Menu.ItemGroup>
                          <Menu.Item>Edit</Menu.Item>
                          <Menu.Item>Copy</Menu.Item>
                          <Menu.Item>Move</Menu.Item>
                        </Menu.ItemGroup>
                        <Menu.ItemGroup>
                          <Menu.Item>Delete</Menu.Item>
                          <Menu.Item>Archive</Menu.Item>
                        </Menu.ItemGroup>
                      </Menu.Content>
                    </Menu.Root>
                  </Box>
                </VStack>
              </VStack>
            </VStack>
          </Box>
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};
