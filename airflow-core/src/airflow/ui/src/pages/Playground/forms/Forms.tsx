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
import { Box, Collapsible, Heading, HStack, Text, VStack } from "@chakra-ui/react";

import { AdvancedControls } from "src/pages/Playground/forms/AdvancedControls";
import { NavigationControls } from "src/pages/Playground/forms/NavigationControls";
import { NumberRangeControls } from "src/pages/Playground/forms/NumberRangeControls";
import { Controls } from "src/pages/Playground/forms/SelectionControls";
import { TextInputs } from "src/pages/Playground/forms/TextInputs";

type FormState = {
  readonly currentPage: number;
  readonly multipleSegmented: Array<string>;
  readonly progress: number;
  readonly radio: string;
  readonly radioCard: string;
  readonly segmented: Array<string>;
  readonly slider: Array<number>;
  readonly switch: boolean;
};

type FormsProps = {
  readonly formState: FormState;
  readonly isOpen: boolean;
  readonly onToggle: () => void;
  readonly updateFormState: (updates: Partial<FormState>) => void;
};

export const Forms = ({ formState, isOpen, onToggle, updateFormState }: FormsProps) => (
  <Box id="forms">
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
            <Heading size="xl">Forms</Heading>
            <Text color="fg.muted" fontSize="sm">
              Form controls, inputs, and interactive components
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
            {/* Basic Form Controls Row */}
            <HStack align="flex-start" flexWrap="wrap" gap={6}>
              <TextInputs />
              <Controls
                radioValue={formState.radio}
                setRadioValue={(value) => updateFormState({ radio: value })}
              />
            </HStack>

            {/* Interactive Controls Row */}
            <HStack align="flex-start" flexWrap="wrap" gap={6}>
              <NumberRangeControls
                setSliderValue={(value) => updateFormState({ slider: value })}
                setSwitchValue={(value) => updateFormState({ switch: value })}
                sliderValue={formState.slider}
                switchValue={formState.switch}
              />
              <NavigationControls
                currentPage={formState.currentPage}
                setCurrentPage={(value) => updateFormState({ currentPage: value })}
              />
            </HStack>

            {/* Advanced Form Controls */}
            <AdvancedControls />
          </VStack>
        </Box>
      </Collapsible.Content>
    </Collapsible.Root>
  </Box>
);
