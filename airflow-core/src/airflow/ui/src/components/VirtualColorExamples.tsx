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

import { Box, Button, Badge, HStack, VStack, Text, Heading } from "@chakra-ui/react";

/**
 * Examples demonstrating Chakra UI v3 Virtual Colors best practices
 * 
 * Key Benefits:
 * 1. Dynamic theming with single colorPalette prop
 * 2. Automatic hover/focus/active states
 * 3. Consistent color patterns across components
 * 4. Easy dark mode support
 * 5. Better maintainability
 */

export const VirtualColorExamples = () => (
    <VStack align="start" gap={6} p={6}>
      <Heading size="lg">Chakra UI v3 Virtual Colors Examples</Heading>
      
      {/* Button Examples */}
      <Box>
        <Text fontSize="lg" fontWeight="bold" mb={3}>Button Components with Virtual Colors</Text>
        <HStack gap={3} wrap="wrap">
          {/* Using recipes with colorPalette */}
          <Button colorPalette="blue" variant="solid">Blue Solid</Button>
          <Button colorPalette="green" variant="outline">Green Outline</Button>
          <Button colorPalette="red" variant="ghost">Red Ghost</Button>
          <Button colorPalette="purple" size="lg" variant="solid">Purple Large</Button>
          
          {/* Danger variant (uses hardcoded red) */}
          <Button variant="danger">Danger Button</Button>
        </HStack>
      </Box>

      {/* Badge Examples */}
      <Box>
        <Text fontSize="lg" fontWeight="bold" mb={3}>Badge Components with Virtual Colors</Text>
        <HStack gap={3} wrap="wrap">
          <Badge colorPalette="blue" variant="solid">Blue Solid</Badge>
          <Badge colorPalette="green" variant="subtle">Green Subtle</Badge>
          <Badge colorPalette="red" variant="outline">Red Outline</Badge>
          <Badge colorPalette="purple" size="md" variant="solid">Purple Medium</Badge>
        </HStack>
      </Box>

      {/* Task State Examples */}
      <Box>
        <Text fontSize="lg" fontWeight="bold" mb={3}>Task State Badges (Dynamic Theming)</Text>
        <HStack gap={3} wrap="wrap">
          <Badge colorPalette="green" variant="solid">Success</Badge>
          <Badge colorPalette="red" variant="solid">Failed</Badge>
          <Badge colorPalette="cyan" variant="solid">Running</Badge>
          <Badge colorPalette="yellow" variant="solid">Queued</Badge>
          <Badge colorPalette="gray" variant="solid">Skipped</Badge>
        </HStack>
      </Box>

      {/* Custom Component Example */}
      <Box>
        <Text fontSize="lg" fontWeight="bold" mb={3}>Custom Stats Card with Virtual Colors</Text>
        <HStack gap={3} wrap="wrap">
          <StatsCardExample colorPalette="red" count={5} label="Failed DAGs" />
          <StatsCardExample colorPalette="green" count={12} label="Success DAGs" />
          <StatsCardExample colorPalette="cyan" count={3} label="Running DAGs" />
          <StatsCardExample colorPalette="yellow" count={8} label="Queued DAGs" />
        </HStack>
      </Box>

      {/* Dark Mode Example */}
      <Box>
        <Text fontSize="lg" fontWeight="bold" mb={3}>Dark Mode Support (Automatic)</Text>
        <Text color="fg.muted" fontSize="sm" mb={3}>
          These components automatically adapt to dark mode using the colorPalette system
        </Text>
        <HStack gap={3} wrap="wrap">
          <Button colorPalette="brand" variant="solid">Brand Button</Button>
          <Button colorPalette="brand" variant="outline">Brand Outline</Button>
          <Badge colorPalette="brand" variant="subtle">Brand Badge</Badge>
        </HStack>
      </Box>
    </VStack>
  );

// Example of a custom component using virtual colors
const StatsCardExample = ({ 
  colorPalette, 
  count, 
  label 
}: { 
  readonly colorPalette: string; 
  readonly count: number; 
  readonly label: string; 
}) => (
  <Box
    _dark={{
      _hover: {
        bg: "colorPalette.800",
        borderColor: "colorPalette.600",
      },
      bg: "colorPalette.900",
      borderColor: "colorPalette.700",
    }}
    _hover={{
      bg: "colorPalette.100",
      borderColor: "colorPalette.300",
    }}
    bg="colorPalette.50"
    borderColor="colorPalette.200"
    borderRadius="lg"
    borderWidth="1px"
    colorPalette={colorPalette}
    cursor="pointer"
    p={3}
  >
    <HStack>
      <Badge colorPalette={colorPalette} variant="solid">
        {count}
      </Badge>
      <Text _dark={{ color: "colorPalette.200" }} color="colorPalette.700" fontSize="sm" fontWeight="bold">
        {label}
      </Text>
    </HStack>
  </Box>
);
