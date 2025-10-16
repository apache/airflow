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
import { Box, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";

type ColorsProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Colors = ({ isOpen, onToggle }: ColorsProps) => {
  // Color palettes available in the theme
  const colorPalettes = [
    { key: "brand", name: "Brand" },
    { key: "gray", name: "Gray" },
    { key: "red", name: "Red" },
    { key: "orange", name: "Orange" },
    { key: "amber", name: "Amber" },
    { key: "yellow", name: "Yellow" },
    { key: "lime", name: "Lime" },
    { key: "green", name: "Green" },
    { key: "emerald", name: "Emerald" },
    { key: "teal", name: "Teal" },
    { key: "cyan", name: "Cyan" },
    { key: "sky", name: "Sky" },
    { key: "blue", name: "Blue" },
    { key: "indigo", name: "Indigo" },
    { key: "violet", name: "Violet" },
    { key: "purple", name: "Purple" },
    { key: "fuchsia", name: "Fuchsia" },
    { key: "pink", name: "Pink" },
    { key: "rose", name: "Rose" },
    { key: "slate", name: "Slate" },
    { key: "zinc", name: "Zinc" },
    { key: "neutral", name: "Neutral" },
    { key: "stone", name: "Stone" },
  ];

  const colorNumbers = ["50", "100", "200", "300", "400", "500", "600", "700", "800", "900", "950"];

  return (
    <Box id="colors">
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
              <Heading size="xl">Color Palette</Heading>
              <Text color="fg.muted" fontSize="sm">
                Complete color system and contrast examples
              </Text>
            </VStack>
            <Text color="brand.solid" fontSize="lg">
              {isOpen ? "−" : "+"}
            </Text>
          </HStack>
        </Collapsible.Trigger>
        <Collapsible.Content>
          <Box borderColor="border.muted" borderTop="none" borderWidth="1px" padding="6">
            <VStack align="stretch" gap="6">
              <Heading size="lg">Color Palette Matrix</Heading>
              <VStack align="stretch" gap={1}>
                {/* Header row with color numbers */}
                <HStack gap={1}>
                  {/* Spacer for color name column */}
                  <Box minWidth="20" />
                  {colorNumbers.map((number) => (
                    <Box key={number} minWidth="12" textAlign="center">
                      <Text fontSize="xs" fontWeight="semibold">
                        {number}
                      </Text>
                    </Box>
                  ))}
                </HStack>

                {/* Color rows */}
                {colorPalettes.map((palette) => (
                  <HStack gap={1} key={palette.key}>
                    {/* Color name label */}
                    <Box minWidth="20" textAlign="right">
                      <Text fontSize="sm" fontWeight="medium">
                        {palette.name.toLowerCase()}
                      </Text>
                    </Box>

                    {/* Color swatches */}
                    {colorNumbers.map((number) => (
                      <Box
                        _hover={{
                          borderColor: "border.emphasized",
                          shadow: "md",
                          transform: "scale(1.1)",
                          zIndex: 10,
                        }}
                        bg={`${palette.key}.${number}`}
                        border="1px solid"
                        borderColor="border.muted"
                        cursor="pointer"
                        height="12"
                        key={`${palette.key}-${number}`}
                        minWidth="12"
                        title={`${palette.key}.${number}`}
                        transition="all 0.2s"
                      />
                    ))}
                  </HStack>
                ))}
              </VStack>

              {/* Black & White Special Colors */}
              <VStack align="stretch" gap={4}>
                <VStack align="stretch" gap={1}>
                  <Heading size="lg">Absolute Colors</Heading>
                  <Text color="fg.muted" fontSize="sm">
                    Pure black and white values
                  </Text>
                </VStack>
                <HStack gap={4}>
                  <VStack gap={2}>
                    <Box bg="black" border="1px solid" borderColor="border.muted" height="16" width="16" />
                    <Text fontSize="sm" fontWeight="medium" textAlign="center">
                      Black
                    </Text>
                  </VStack>
                  <VStack gap={2}>
                    <Box bg="white" border="1px solid" borderColor="border.muted" height="16" width="16" />
                    <Text fontSize="sm" fontWeight="medium" textAlign="center">
                      White
                    </Text>
                  </VStack>
                </HStack>
              </VStack>
            </VStack>
          </Box>
        </Collapsible.Content>
      </Collapsible.Root>
    </Box>
  );
};
