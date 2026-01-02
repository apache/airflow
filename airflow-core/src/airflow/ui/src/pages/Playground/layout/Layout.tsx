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
 * KIND, either express or implied.  See the License for the specific
 * language governing permissions and limitations under the License.
 */
import { Box, Collapsible, Heading, HStack, Text, VStack, Accordion } from "@chakra-ui/react";

type LayoutProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Layout = ({ isOpen, onToggle }: LayoutProps) => (
  <Box id="layout">
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
            <Heading size="xl">Layout</Heading>
            <Text color="fg.muted" fontSize="sm">
              Box, Container, Flex, Grid, Stack, and other layout components
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
            <Text color="fg.muted" fontSize="sm">
              Layout components are fundamental building blocks for structuring your UI. 
              They provide spacing, alignment, and responsive behavior.
            </Text>
            
            {/* Box Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Box Component</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Basic Box</Text>
                <Box bg="brand.muted" borderRadius="md" p={4}>
                  <Text>This is a basic box with background and padding</Text>
                </Box>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Box with Border</Text>
                <Box border="1px solid" borderColor="border.emphasized" borderRadius="md" p={4}>
                  <Text>Box with border and rounded corners</Text>
                </Box>
              </VStack>
            </VStack>

            {/* Container Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Container Component</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Responsive Container</Text>
                <Box border="1px solid" borderColor="border.emphasized" borderRadius="md" p={4}>
                  <Text>Container automatically adjusts width based on screen size</Text>
                </Box>
              </VStack>
            </VStack>

            {/* Stack Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Stack Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">VStack (Vertical Stack)</Text>
                <VStack bg="bg.muted" borderRadius="md" gap={2} p={4}>
                  <Box bg="brand.solid" borderRadius="sm" color="white" p={2} textAlign="center" width="full">
                    Item 1
                  </Box>
                  <Box bg="brand.solid" borderRadius="sm" color="white" p={2} textAlign="center" width="full">
                    Item 2
                  </Box>
                  <Box bg="brand.solid" borderRadius="sm" color="white" p={2} textAlign="center" width="full">
                    Item 3
                  </Box>
                </VStack>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">HStack (Horizontal Stack)</Text>
                <HStack bg="bg.muted" borderRadius="md" gap={2} p={4}>
                  <Box bg="green.500" borderRadius="sm" color="white" flex="1" p={2} textAlign="center">
                    Item 1
                  </Box>
                  <Box bg="green.500" borderRadius="sm" color="white" flex="1" p={2} textAlign="center">
                    Item 2
                  </Box>
                  <Box bg="green.500" borderRadius="sm" color="white" flex="1" p={2} textAlign="center">
                    Item 3
                  </Box>
                </HStack>
              </VStack>
            </VStack>

            {/* Grid Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Grid Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Simple Grid</Text>
                <Box
                  bg="bg.muted"
                  borderRadius="md"
                  display="grid"
                  gap={2}
                  gridTemplateColumns="repeat(3, 1fr)"
                  p={4}
                >
                  <Box bg="blue.500" borderRadius="sm" color="white" p={2} textAlign="center">
                    Grid 1
                  </Box>
                  <Box bg="blue.500" borderRadius="sm" color="white" p={2} textAlign="center">
                    Grid 2
                  </Box>
                  <Box bg="blue.500" borderRadius="sm" color="white" p={2} textAlign="center">
                    Grid 3
                  </Box>
                  <Box bg="blue.500" borderRadius="sm" color="white" p={2} textAlign="center">
                    Grid 4
                  </Box>
                  <Box bg="blue.500" borderRadius="sm" color="white" p={2} textAlign="center">
                    Grid 5
                  </Box>
                  <Box bg="blue.500" borderRadius="sm" color="white" p={2} textAlign="center">
                    Grid 6
                  </Box>
                </Box>
              </VStack>
            </VStack>

            {/* Flex Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Flex Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Flex with Justify Content</Text>
                <Box bg="bg.muted" borderRadius="md" display="flex" gap={2} justifyContent="space-between" p={4}>
                  <Box bg="purple.500" borderRadius="sm" color="white" p={2}>
                    Start
                  </Box>
                  <Box bg="purple.500" borderRadius="sm" color="white" p={2}>
                    Center
                  </Box>
                  <Box bg="purple.500" borderRadius="sm" color="white" p={2}>
                    End
                  </Box>
                </Box>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Flex with Align Items</Text>
                <Box
                  alignItems="center"
                  bg="bg.muted"
                  borderRadius="md"
                  display="flex"
                  gap={2}
                  height="100px"
                  p={4}
                >
                  <Box bg="orange.500" borderRadius="sm" color="white" p={2}>
                    Aligned Item
                  </Box>
                  <Box bg="orange.500" borderRadius="sm" color="white" p={2}>
                    Another Item
                  </Box>
                </Box>
              </VStack>
            </VStack>

            {/* Accordion Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Accordion Component</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Collapsible Sections</Text>
                <Accordion.Root>
                  <Accordion.Item value="item1">
                    <Accordion.ItemTrigger>
                      <Text fontWeight="semibold">Section 1: Basic Information</Text>
                    </Accordion.ItemTrigger>
                    <Accordion.ItemContent>
                      <Text color="fg.muted">
                        This is the content for section 1. It can contain any layout components
                        and will expand/collapse when clicked.
                      </Text>
                    </Accordion.ItemContent>
                  </Accordion.Item>
                  <Accordion.Item value="item2">
                    <Accordion.ItemTrigger>
                      <Text fontWeight="semibold">Section 2: Advanced Settings</Text>
                    </Accordion.ItemTrigger>
                    <Accordion.ItemContent>
                      <VStack align="stretch" gap={2}>
                        <Text color="fg.muted">Advanced configuration options:</Text>
                        <Box bg="bg.muted" borderRadius="sm" p={2}>
                          <Text fontSize="sm">Setting 1: Enabled</Text>
                        </Box>
                        <Box bg="bg.muted" borderRadius="sm" p={2}>
                          <Text fontSize="sm">Setting 2: Disabled</Text>
                        </Box>
                      </VStack>
                    </Accordion.ItemContent>
                  </Accordion.Item>
                  <Accordion.Item value="item3">
                    <Accordion.ItemTrigger>
                      <Text fontWeight="semibold">Section 3: Documentation</Text>
                    </Accordion.ItemTrigger>
                    <Accordion.ItemContent>
                      <Text color="fg.muted">
                        Documentation and help content can be placed in accordion sections
                        to keep the interface clean and organized.
                      </Text>
                    </Accordion.ItemContent>
                  </Accordion.Item>
                </Accordion.Root>
              </VStack>
            </VStack>
          </VStack>
        </Box>
      </Collapsible.Content>
    </Collapsible.Root>
  </Box>
);
