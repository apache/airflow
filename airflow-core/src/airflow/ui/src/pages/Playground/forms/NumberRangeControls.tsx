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
import { Card, Field, Heading, PinInput, Slider, Text, VStack } from "@chakra-ui/react";
import { useState } from "react";

import { NumberInputField, NumberInputRoot, Switch } from "src/components/ui";

type NumberRangeControlsProps = {
  readonly setSliderValue: (value: Array<number>) => void;
  readonly setSwitchValue: (value: boolean) => void;
  readonly sliderValue: Array<number>;
  readonly switchValue: boolean;
};

export const NumberRangeControls = ({
  setSliderValue,
  setSwitchValue,
  sliderValue,
  switchValue,
}: NumberRangeControlsProps) => {
  const [numberValue, setNumberValue] = useState("0");
  const [pinValue, setPinValue] = useState("");

  return (
    <Card.Root flex="1" minWidth="300px">
      <Card.Header>
        <Heading size="lg">Number & Range Controls</Heading>
        <Text color="fg.muted" fontSize="sm">
          Numeric inputs and sliders
        </Text>
      </Card.Header>
      <Card.Body>
        <VStack align="stretch" gap={4}>
          <Field.Root>
            <Field.Label>Number Input</Field.Label>
            <NumberInputRoot
              max={100}
              min={0}
              onValueChange={(details) => setNumberValue(details.value)}
              value={numberValue}
            >
              <NumberInputField />
            </NumberInputRoot>
          </Field.Root>

          <Field.Root>
            <Field.Label>Range Slider</Field.Label>
            <Slider.Root
              max={100}
              min={0}
              onValueChange={(details) => setSliderValue(details.value)}
              step={1}
              value={sliderValue}
            >
              <Slider.Control>
                <Slider.Track>
                  <Slider.Range />
                </Slider.Track>
                <Slider.Thumb index={0} />
              </Slider.Control>
              <Slider.ValueText>Value: {sliderValue[0]}</Slider.ValueText>
            </Slider.Root>
          </Field.Root>

          <Field.Root>
            <Field.Label>PIN Input</Field.Label>
            <PinInput.Root
              onValueChange={(details) => setPinValue(details.value.join(""))}
              value={[...pinValue].slice(0, 4)}
            >
              <PinInput.Control>
                {[0, 1, 2, 3].map((index) => (
                  <PinInput.Input index={index} key={index} />
                ))}
              </PinInput.Control>
            </PinInput.Root>
            <Field.HelperText>Enter 4-digit PIN</Field.HelperText>
          </Field.Root>

          <Field.Root>
            <Field.Label>Switch Control</Field.Label>
            <Switch
              checked={switchValue}
              onCheckedChange={(details) => setSwitchValue(Boolean(details.checked))}
            >
              Toggle setting
            </Switch>
          </Field.Root>
        </VStack>
      </Card.Body>
    </Card.Root>
  );
};
