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
  Checkbox,
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

type SelectionControlsProps = {
  readonly radioValue: string;
  readonly setRadioValue: (value: string) => void;
};

export const SelectionControls = ({ radioValue, setRadioValue }: SelectionControlsProps) => {
  const [selectValue, setSelectValue] = useState("option1");
  const [checkboxChecked, setCheckboxChecked] = useState(false);

  return (
    <Card.Root flex="1" minWidth="300px">
      <Card.Header>
        <Heading size="lg">Selection Controls</Heading>
        <Text color="fg.muted" fontSize="sm">
          Dropdowns, radios, and checkboxes
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
                    <Checkbox.Root
                      checked={checkboxChecked}
                      onCheckedChange={(details) => setCheckboxChecked(Boolean(details.checked))}
                      size="sm"
                    >
                      <Checkbox.HiddenInput />
                      <Checkbox.Control />
                      <Checkbox.Label>Checkbox option</Checkbox.Label>
                    </Checkbox.Root>

                    <Checkbox.Root defaultChecked size="sm">
                      <Checkbox.HiddenInput />
                      <Checkbox.Control />
                      <Checkbox.Label>Pre-checked option</Checkbox.Label>
                    </Checkbox.Root>

                    <Checkbox.Root disabled size="sm">
                      <Checkbox.HiddenInput />
                      <Checkbox.Control />
                      <Checkbox.Label>Disabled option</Checkbox.Label>
                    </Checkbox.Root>
                  </VStack>
                </Fieldset.Content>
              </Fieldset.Root>
            </VStack>
          </HStack>
        </VStack>
      </Card.Body>
    </Card.Root>
  );
};
