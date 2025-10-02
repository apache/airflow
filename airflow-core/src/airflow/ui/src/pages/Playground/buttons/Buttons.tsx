/* eslint-disable i18next/no-literal-string */

/* eslint-disable react/jsx-max-depth */

/* eslint-disable @typescript-eslint/no-unsafe-assignment */

/* eslint-disable @typescript-eslint/no-explicit-any */

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
import { Box, Code, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";

import { Button } from "src/components/ui";

type ButtonsProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Buttons = ({ isOpen, onToggle }: ButtonsProps) => {
  const colorPalettes = ["brand", "gray", "red", "green", "blue", "yellow", "purple"];

  return (
    <Box id="buttons">
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
              <Heading size="xl">Buttons & Code</Heading>
              <Text color="fg.muted" fontSize="sm">
                Interactive elements and code display
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
              {/* Button Variants */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Button Variants</Heading>
                <VStack align="stretch" gap={4}>
                  {["solid", "outline", "ghost", "subtle"].map((variant) => (
                    <Box key={variant}>
                      <Text fontSize="sm" fontWeight="semibold">
                        {variant}
                      </Text>
                      <HStack gap={3} wrap="wrap">
                        {colorPalettes.map((palette) => (
                          <Button
                            colorPalette={palette}
                            key={`${variant}-${palette}`}
                            size="sm"
                            variant={variant as any}
                          >
                            {palette}
                          </Button>
                        ))}
                      </HStack>
                    </Box>
                  ))}
                </VStack>
              </VStack>

              {/* Button Sizes */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Button Sizes</Heading>
                <HStack gap={3} wrap="wrap">
                  <Button colorPalette="brand" size="xs">
                    Extra Small
                  </Button>
                  <Button colorPalette="brand" size="sm">
                    Small
                  </Button>
                  <Button colorPalette="brand" size="md">
                    Medium
                  </Button>
                  <Button colorPalette="brand" size="lg">
                    Large
                  </Button>
                  <Button colorPalette="brand" size="xl">
                    Extra Large
                  </Button>
                </HStack>
              </VStack>

              {/* Code Display */}
              <VStack align="stretch" gap={4}>
                <Heading size="lg">Code Display</Heading>
                <VStack align="stretch" gap={4}>
                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Inline Code
                    </Text>
                    <Text>
                      Use <Code>npm install</Code> to install dependencies, then run <Code>npm start</Code> to
                      start the development server.
                    </Text>
                  </Box>

                  <Box>
                    <Text fontSize="sm" fontWeight="semibold">
                      Code Block
                    </Text>
                    <Code display="block" overflow="auto" whiteSpace="pre">
                      {`function hello() {
  console.log("Hello, World!");
  return "Hello from Airflow UI";
}`}
                    </Code>
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
