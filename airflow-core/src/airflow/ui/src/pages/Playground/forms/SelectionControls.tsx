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
import {
  Card,
  Field,
  Fieldset,
  Heading,
  HStack,
  NativeSelect,
  RadioGroup,
  Text,
  VStack,
} from "@chakra-ui/react";
import { useState } from "react";

import { Checkbox, RadioCardItem, RadioCardLabel, RadioCardRoot, SegmentedControl } from "src/components/ui";

type ControlsProps = {
  readonly radioValue: string;
  readonly setRadioValue: (value: string) => void;
};

export const Controls = ({ radioValue, setRadioValue }: ControlsProps) => {
  const [selectValue, setSelectValue] = useState("option1");
  const [checkboxChecked, setCheckboxChecked] = useState(false);
  const [radioCardValue, setRadioCardValue] = useState("option1");
  const [segmentedValue, setSegmentedValue] = useState<Array<string>>(["option1"]);
  const [multipleSegmentedValue, setMultipleSegmentedValue] = useState<Array<string>>(["option1", "option2"]);

  return (
    <Card.Root flex="1" minWidth="300px">
      <Card.Header>
        <Heading size="lg">Controls</Heading>
        <Text color="fg.muted" fontSize="sm">
          All form controls including dropdowns, radios, checkboxes, RadioCard, and SegmentedControl
        </Text>
      </Card.Header>
      <Card.Body>
        <VStack align="stretch" gap={4}>
          <Field.Root>
            <Field.Label>Select Dropdown</Field.Label>
            <NativeSelect.Root>
              <NativeSelect.Field
                onChange={(event) => setSelectValue(event.currentTarget.value)}
                placeholder="Choose an option"
                value={selectValue}
              >
                <option value="option1">Option 1</option>
                <option value="option2">Option 2</option>
                <option value="option3">Option 3</option>
                <option value="option4">Option 4</option>
              </NativeSelect.Field>
            </NativeSelect.Root>
          </Field.Root>

          {/* Radio Group and Checkboxes in Two Columns */}
          <HStack align="flex-start" gap={6} width="full">
            {/* Radio Group Column */}
            <VStack align="stretch" flex="1" gap={4}>
              <Fieldset.Root>
                <Fieldset.Legend>Radio Group</Fieldset.Legend>
                <Fieldset.Content>
                  <RadioGroup.Root
                    onValueChange={(details) => setRadioValue(details.value ?? "")}
                    value={radioValue}
                  >
                    <VStack align="stretch" gap={3}>
                      <RadioGroup.Item value="option1">
                        <RadioGroup.ItemHiddenInput />
                        <RadioGroup.ItemIndicator />
                        <RadioGroup.ItemText>First option</RadioGroup.ItemText>
                      </RadioGroup.Item>
                      <RadioGroup.Item value="option2">
                        <RadioGroup.ItemHiddenInput />
                        <RadioGroup.ItemIndicator />
                        <RadioGroup.ItemText>Second option</RadioGroup.ItemText>
                      </RadioGroup.Item>
                      <RadioGroup.Item value="option3">
                        <RadioGroup.ItemHiddenInput />
                        <RadioGroup.ItemIndicator />
                        <RadioGroup.ItemText>Third option</RadioGroup.ItemText>
                      </RadioGroup.Item>
                    </VStack>
                  </RadioGroup.Root>
                </Fieldset.Content>
              </Fieldset.Root>
            </VStack>

            {/* Checkboxes Column */}
            <VStack align="stretch" flex="1" gap={4}>
              <Fieldset.Root>
                <Fieldset.Legend>Checkboxes</Fieldset.Legend>
                <Fieldset.Content>
                  <VStack align="flex-start" gap={3}>
                    <Checkbox
                      checked={checkboxChecked}
                      onCheckedChange={(details) => setCheckboxChecked(Boolean(details.checked))}
                      size="sm"
                    >
                      Checkbox option
                    </Checkbox>

                    <Checkbox defaultChecked size="sm">
                      Pre-checked option
                    </Checkbox>

                    <Checkbox disabled size="sm">
                      Disabled option
                    </Checkbox>
                  </VStack>
                </Fieldset.Content>
              </Fieldset.Root>
            </VStack>
          </HStack>

          {/* RadioCard Component */}
          <Fieldset.Root>
            <Fieldset.Legend>RadioCard Component</Fieldset.Legend>
            <Fieldset.Content>
              <RadioCardRoot
                defaultValue="option1"
                onValueChange={(details) => setRadioCardValue(details.value ?? "option1")}
                value={radioCardValue}
              >
                <RadioCardLabel fontSize="md" fontWeight="semibold" mb={3}>
                  Choose deployment strategy
                </RadioCardLabel>
                <RadioCardItem
                  description="Deploy immediately to production"
                  label="Immediate"
                  value="option1"
                />
                <RadioCardItem
                  description="Deploy to staging first, then production"
                  label="Staged"
                  value="option2"
                />
                <RadioCardItem
                  description="Deploy with blue-green strategy"
                  label="Blue-Green"
                  value="option3"
                />
              </RadioCardRoot>
            </Fieldset.Content>
          </Fieldset.Root>

          {/* SegmentedControl Components */}
          <Fieldset.Root>
            <Fieldset.Legend>SegmentedControl Components</Fieldset.Legend>
            <Fieldset.Content>
              <VStack align="stretch" gap={4}>
                <VStack align="stretch" gap={2}>
                  <Text fontSize="sm" fontWeight="semibold">
                    Single Selection
                  </Text>
                  <SegmentedControl
                    defaultValues={["option1"]}
                    multiple={false}
                    onChange={(value) => {
                      setSegmentedValue(value);
                      console.log("Single selection changed:", value);
                    }}
                    options={[
                      { label: "All", value: "option1" },
                      { label: "Active", value: "option2" },
                      { label: "Paused", value: "option3" },
                    ]}
                  />
                  <Text color="fg.muted" fontSize="xs">
                    Selected: {segmentedValue.join(", ")}
                  </Text>
                </VStack>

                <VStack align="stretch" gap={2}>
                  <Text fontSize="sm" fontWeight="semibold">
                    Multiple Selection
                  </Text>
                  <SegmentedControl
                    defaultValues={["option1", "option2"]}
                    multiple
                    onChange={(value) => {
                      setMultipleSegmentedValue(value);
                      console.log("Multiple selection changed:", value);
                    }}
                    options={[
                      { label: "Past", value: "option1" },
                      { label: "Future", value: "option2" },
                      { label: "Upstream", value: "option3" },
                      { label: "Downstream", value: "option4" },
                    ]}
                  />
                  <Text color="fg.muted" fontSize="xs">
                    Selected: {multipleSegmentedValue.join(", ")}
                  </Text>
                </VStack>

                <VStack align="stretch" gap={2}>
                  <Text fontSize="sm" fontWeight="semibold">
                    With Disabled Options
                  </Text>
                  <SegmentedControl
                    defaultValues={["enabled"]}
                    multiple={false}
                    onChange={() => {
                      // Handle segmented control change
                    }}
                    options={[
                      { label: "Enabled", value: "enabled" },
                      { disabled: true, label: "Disabled", value: "disabled" },
                      { label: "Another", value: "another" },
                    ]}
                  />
                </VStack>
              </VStack>
            </Fieldset.Content>
          </Fieldset.Root>
        </VStack>
      </Card.Body>
    </Card.Root>
  );
};
