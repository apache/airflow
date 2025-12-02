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
import { Box, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";

type TypographyProps = {
  readonly isOpen: boolean;
  readonly onToggle: () => void;
};

export const Typography = ({ isOpen, onToggle }: TypographyProps) => (
  <Box id="typography">
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
            <Heading size="xl">Typography</Heading>
            <Text color="fg.muted" fontSize="sm">
              Text, Heading, and other typography components
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
              Typography components help you create consistent text styling throughout your application.
            </Text>
            
            {/* Heading Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Heading Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Heading Sizes</Text>
                <VStack align="stretch" gap={2}>
                  <Heading size="4xl">Heading 4xl</Heading>
                  <Heading size="3xl">Heading 3xl</Heading>
                  <Heading size="2xl">Heading 2xl</Heading>
                  <Heading size="xl">Heading xl</Heading>
                  <Heading size="lg">Heading lg</Heading>
                  <Heading size="md">Heading md</Heading>
                  <Heading size="sm">Heading sm</Heading>
                  <Heading size="xs">Heading xs</Heading>
                </VStack>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Heading with Semantic HTML</Text>
                <VStack align="stretch" gap={2}>
                  <Heading as="h1" size="xl">Page Title (h1)</Heading>
                  <Heading as="h2" size="lg">Section Title (h2)</Heading>
                  <Heading as="h3" size="md">Subsection Title (h3)</Heading>
                  <Heading as="h4" size="sm">Sub-subsection Title (h4)</Heading>
                </VStack>
              </VStack>
            </VStack>

            {/* Text Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Text Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Text Sizes</Text>
                <VStack align="stretch" gap={1}>
                  <Text fontSize="xs">Extra small text (xs)</Text>
                  <Text fontSize="sm">Small text (sm)</Text>
                  <Text fontSize="md">Medium text (md) - default</Text>
                  <Text fontSize="lg">Large text (lg)</Text>
                  <Text fontSize="xl">Extra large text (xl)</Text>
                  <Text fontSize="2xl">2x large text (2xl)</Text>
                </VStack>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Text Variants</Text>
                <VStack align="stretch" gap={1}>
                  <Text>Regular text</Text>
                  <Text fontWeight="bold">Bold text</Text>
                  <Text fontStyle="italic">Italic text</Text>
                  <Text textDecoration="underline">Underlined text</Text>
                  <Text textDecoration="line-through">Strikethrough text</Text>
                </VStack>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Text Colors</Text>
                <VStack align="stretch" gap={1}>
                  <Text color="fg.default">Default text color</Text>
                  <Text color="fg.muted">Muted text color</Text>
                  <Text color="fg.subtle">Subtle text color</Text>
                  <Text color="brand.solid">Brand text color</Text>
                  <Text color="red.500">Error text color</Text>
                  <Text color="green.500">Success text color</Text>
                </VStack>
              </VStack>
            </VStack>

            {/* Code Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Code Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Inline Code</Text>
                <Text>
                  Use <Text as="code" bg="bg.muted" borderRadius="sm" px={1}>useState</Text> hook for state management in React components.
                </Text>
              </VStack>

              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Code Block</Text>
                <Box
                  as="pre"
                  bg="bg.muted"
                  borderRadius="md"
                  fontFamily="mono"
                  fontSize="sm"
                  overflow="auto"
                  p={4}
                  whiteSpace="pre-wrap"
                >
                  <Text as="code" color="fg.default">
{`const MyComponent = () => {
  const [count, setCount] = useState(0);
  
  return (
    <div>
      <p>Count: {count}</p>
      <button onClick={() => setCount(count + 1)}>
        Increment
      </button>
    </div>
  );
};`}
                  </Text>
                </Box>
              </VStack>
            </VStack>

            {/* Link Examples */}
            <VStack align="stretch" gap={4}>
              <Heading size="lg">Link Components</Heading>
              <VStack align="stretch" gap={2}>
                <Text fontSize="sm" fontWeight="semibold">Link Variants</Text>
                <VStack align="stretch" gap={1}>
                  <Text as="a" color="brand.solid" href="#" textDecoration="underline">
                    Default link
                  </Text>
                  <Text _hover={{ textDecoration: "underline" }} as="a" color="brand.solid" href="#" textDecoration="none">
                    Hover underline link
                  </Text>
                  <Text as="a" color="fg.muted" href="#" textDecoration="underline">
                    Muted link
                  </Text>
                </VStack>
              </VStack>
            </VStack>
          </VStack>
        </Box>
      </Collapsible.Content>
    </Collapsible.Root>
  </Box>
);
