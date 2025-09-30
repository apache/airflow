/* eslint-disable i18next/no-literal-string */



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
import { Card, Field, Heading, Text, VStack } from "@chakra-ui/react";

import { Pagination } from "src/components/ui";

type NavigationControlsProps = {
  readonly currentPage: number;
  readonly setCurrentPage: (page: number) => void;
};

export const NavigationControls = ({ currentPage, setCurrentPage }: NavigationControlsProps) => (
  <Card.Root flex="1" minWidth="300px">
    <Card.Header>
      <Heading size="lg">Navigation Controls</Heading>
      <Text color="fg.muted" fontSize="sm">
        Pagination and navigation
      </Text>
    </Card.Header>
    <Card.Body>
      <VStack align="stretch" gap={4}>
        <Field.Root>
          <Field.Label>Pagination Component</Field.Label>
          <Pagination.Root
            count={50}
            onPageChange={(details) => setCurrentPage(details.page)}
            page={currentPage}
            pageSize={5}
            siblingCount={1}
          >
            <Pagination.PrevTrigger />
            <Pagination.Items />
            <Pagination.NextTrigger />
          </Pagination.Root>
          <Field.HelperText>Page {currentPage} of 10</Field.HelperText>
        </Field.Root>

        <VStack align="stretch" gap={3}>
          <Text fontSize="sm" fontWeight="semibold">
            Form Accessibility Features:
          </Text>
          <VStack align="stretch" gap={2}>
            <Text fontSize="sm">• Proper labeling for all inputs</Text>
            <Text fontSize="sm">• ARIA attributes for screen readers</Text>
            <Text fontSize="sm">• Keyboard navigation support</Text>
            <Text fontSize="sm">• Focus management and indicators</Text>
            <Text fontSize="sm">• Error state announcements</Text>
            <Text fontSize="sm">• Helper text associations</Text>
          </VStack>
        </VStack>

        <VStack align="stretch" gap={3}>
          <Text fontSize="sm" fontWeight="semibold">
            Keyboard Shortcuts:
          </Text>
          <VStack align="stretch" gap={2}>
            <Text fontSize="sm">• Tab: Navigate between fields</Text>
            <Text fontSize="sm">• Space: Toggle checkboxes/switches</Text>
            <Text fontSize="sm">• Arrow keys: Navigate options</Text>
            <Text fontSize="sm">• Enter: Submit or activate</Text>
            <Text fontSize="sm">• Escape: Close dropdowns</Text>
          </VStack>
        </VStack>
      </VStack>
    </Card.Body>
  </Card.Root>
);
