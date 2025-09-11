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
import { useState, useEffect, forwardRef } from "react";

import { NumberInputField, NumberInputRoot } from "src/components/ui/NumberInput";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";

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

export const NumberFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const hasValue = filter.value !== null && filter.value !== undefined && filter.value !== "";

  const [inputValue, setInputValue] = useState(filter.value?.toString() ?? "");

  useEffect(() => {
    setInputValue(filter.value?.toString() ?? "");
  }, [filter.value]);

  const handleValueChange = ({ value }: { value: string }) => {
    setInputValue(value);

    if (value === "") {
      onChange(undefined);

      return;
    }

    // Allow user to input negative sign for negative number
    if (value === "-") {
      return;
    }

    const parsedValue = Number(value);

    if (!isNaN(parsedValue)) {
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
        max={filter.config.max}
        min={filter.config.min}
        onValueChange={handleValueChange}
        placeholder={filter.config.placeholder}
        value={inputValue}
      />
    </FilterPill>
  );
};
