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
import { Switch as ChakraSwitch } from "@chakra-ui/react";
import { forwardRef } from "react";

export type SwitchProps = {
  readonly inputProps?: React.InputHTMLAttributes<HTMLInputElement>;
  readonly rootRef?: React.Ref<HTMLLabelElement>;
  readonly thumbLabel?: { off: React.ReactNode; on: React.ReactNode };
  readonly trackLabel?: { off: React.ReactNode; on: React.ReactNode };
} & ChakraSwitch.RootProps;

export const Switch = forwardRef<HTMLInputElement, SwitchProps>((props, ref) => {
  const { children, inputProps, rootRef, thumbLabel, trackLabel, ...rest } = props;

  return (
    <ChakraSwitch.Root ref={rootRef} {...rest}>
      <ChakraSwitch.HiddenInput ref={ref} {...inputProps} />
      <ChakraSwitch.Control>
        <ChakraSwitch.Thumb>
          {thumbLabel ? (
            <ChakraSwitch.ThumbIndicator fallback={thumbLabel.off}>
              {thumbLabel.on}
            </ChakraSwitch.ThumbIndicator>
          ) : undefined}
        </ChakraSwitch.Thumb>
        {trackLabel ? (
          <ChakraSwitch.Indicator fallback={trackLabel.off}>{trackLabel.on}</ChakraSwitch.Indicator>
        ) : undefined}
      </ChakraSwitch.Control>
      {Boolean(children) && <ChakraSwitch.Label>{children}</ChakraSwitch.Label>}
    </ChakraSwitch.Root>
  );
});
