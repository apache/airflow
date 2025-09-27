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
import { useRef } from "react";
import { useHotkeys } from "react-hotkeys-hook";

import { InputWithAddon } from "../../ui";
import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";

export const TextSearchFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const inputRef = useRef<HTMLInputElement>(null);

  const hasValue = filter.value !== null && filter.value !== undefined && String(filter.value).trim() !== "";

  const handleInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const newValue = event.target.value;

    onChange(newValue || undefined);
  };

  useHotkeys(
    "mod+k",
    () => {
      if (!filter.config.hotkeyDisabled) {
        inputRef.current?.focus();
      }
    },
    { enabled: !filter.config.hotkeyDisabled, preventDefault: true },
  );

  return (
    <FilterPill
      displayValue={hasValue ? String(filter.value) : ""}
      filter={filter}
      hasValue={hasValue}
      onChange={onChange}
      onRemove={onRemove}
    >
      <InputWithAddon
        label={filter.config.label}
        onChange={handleInputChange}
        placeholder={filter.config.placeholder}
        value={String(filter.value ?? "")}
      />
    </FilterPill>
  );
};
