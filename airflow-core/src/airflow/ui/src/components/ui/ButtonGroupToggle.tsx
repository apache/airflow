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
import type { ButtonGroupProps } from "@chakra-ui/react";
import { Button, ButtonGroup } from "@chakra-ui/react";
import type { ReactNode } from "react";

export type ButtonGroupOption<T extends string = string> = {
  readonly disabled?: boolean;
  readonly label: ((isSelected: boolean) => ReactNode) | ReactNode;
  readonly value: T;
};

type ButtonGroupToggleProps<T extends string = string> = {
  readonly onChange: (value: T) => void;
  readonly options: Array<ButtonGroupOption<T>>;
  readonly value: T;
} & Omit<ButtonGroupProps, "onChange">;

export const ButtonGroupToggle = <T extends string = string>({
  onChange,
  options,
  value,
  ...rest
}: ButtonGroupToggleProps<T>) => (
  <ButtonGroup attached colorPalette="brand" size="sm" variant="outline" {...rest}>
    {options.map((option) => {
      const isSelected = option.value === value;
      const label = typeof option.label === "function" ? option.label(isSelected) : option.label;

      return (
        <Button
          disabled={option.disabled}
          key={option.value}
          onClick={() => onChange(option.value)}
          value={option.value}
          variant={isSelected ? "solid" : "outline"}
        >
          {label}
        </Button>
      );
    })}
  </ButtonGroup>
);
