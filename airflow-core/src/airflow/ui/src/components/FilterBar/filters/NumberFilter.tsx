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
import { useEffect, useState, forwardRef } from "react";
import type React from "react";

import { NumberInputField, NumberInputRoot } from "src/components/ui/NumberInput";

import { FilterPill } from "../FilterPill";
import type { FilterConfig, FilterPluginProps } from "../types";

const NumberInputWithRef = forwardRef<
  HTMLInputElement,
  {
    readonly max?: number;
    readonly min?: number;
    readonly onBlur?: () => void;
    readonly onFocus?: () => void;
    readonly onKeyDown?: (event: React.KeyboardEvent) => void;
    readonly onValueChange: (details: { value: string }) => void;
    readonly placeholder?: string;
    readonly value: string;
  }
>((props, ref) => {
  const { max, min, onBlur, onFocus, onKeyDown, onValueChange, placeholder, value } = props;

  return (
    <NumberInputRoot
      borderRadius="full"
      max={max}
      min={min}
      onValueChange={onValueChange}
      overflow="hidden"
      value={value}
      width="180px"
    >
      <NumberInputField
        borderRadius="full"
        onBlur={onBlur}
        onFocus={onFocus}
        onKeyDown={onKeyDown}
        placeholder={placeholder}
        ref={ref}
      />
    </NumberInputRoot>
  );
});

NumberInputWithRef.displayName = "NumberInputWithRef";

type NumberConfig = Extract<FilterConfig, { type: "number" }>;

export const NumberFilter = ({ filter, onChange, onRemove }: FilterPluginProps): JSX.Element => {
  const numberConfig = filter.config as NumberConfig;
  const { max, min, placeholder } = numberConfig;

  const hasValue = filter.value !== undefined && filter.value !== "";

  const [inputValue, setInputValue] = useState<string>(filter.value?.toString() ?? "");

  useEffect(() => {
    setInputValue(filter.value?.toString() ?? "");
  }, [filter.value]);

  const handleValueChange = ({ value }: { value: string }) => {
    setInputValue(value);

    if (value === "") {
      onChange(undefined);

      return;
    }

    // Allows users to input negative sign for negative number
    if (value === "-") {
      return;
    }

    const parsedValue = Number(value);

    if (!Number.isNaN(parsedValue)) {
      onChange(parsedValue);
    }
  };

  return (
    <FilterPill
      displayValue={hasValue ? String(filter.value) : ""}
      filter={filter}
      hasValue={hasValue}
      onChange={onChange}
      onRemove={onRemove}
    >
      <NumberInputWithRef
        max={max}
        min={min}
        onValueChange={handleValueChange}
        placeholder={placeholder}
        value={inputValue}
      />
    </FilterPill>
  );
};
