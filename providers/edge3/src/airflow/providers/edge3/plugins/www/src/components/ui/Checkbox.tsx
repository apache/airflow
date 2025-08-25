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
import { Checkbox as ChakraCheckbox } from "@chakra-ui/react";
import * as React from "react";

export type CheckboxProps = {
  readonly icon?: React.ReactNode;
  readonly inputProps?: React.InputHTMLAttributes<HTMLInputElement>;
  readonly rootRef?: React.Ref<HTMLLabelElement>;
} & ChakraCheckbox.RootProps;

export const Checkbox = React.forwardRef<HTMLInputElement, CheckboxProps>((props, ref) => {
  const { children, icon, inputProps, rootRef, ...rest } = props;

  return (
    <ChakraCheckbox.Root ref={rootRef} {...rest}>
      <ChakraCheckbox.HiddenInput ref={ref} {...inputProps} />
      <ChakraCheckbox.Control>{icon ?? <ChakraCheckbox.Indicator />}</ChakraCheckbox.Control>
      {children !== undefined && <ChakraCheckbox.Label>{children}</ChakraCheckbox.Label>}
    </ChakraCheckbox.Root>
  );
});
