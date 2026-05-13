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
import { CloseButton, HStack, Input, InputGroup, Kbd, type InputGroupProps } from "@chakra-ui/react";
import { useEffect, useRef, useState, type ChangeEvent } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiSearch } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

import { AdvancedSearchToggle, type AdvancedSearchToggleProps } from "src/components/AdvancedSearchToggle";
import { getMetaKey } from "src/utils";

const debounceDelay = 200;

type AdvancedSearchProps = Omit<AdvancedSearchToggleProps, "size">;

type Props = {
  readonly advancedSearch?: AdvancedSearchProps;
  readonly defaultValue: string;
  readonly hotkeyDisabled?: boolean;
  readonly onChange: (value: string) => void;
  readonly placeholder: string;
} & Omit<InputGroupProps, "children" | "onChange">;

export const SearchBar = ({
  advancedSearch,
  defaultValue,
  hotkeyDisabled = false,
  onChange,
  placeholder,
  ...props
}: Props) => {
  const lastSentValue = useRef(defaultValue);
  const handleSearchChange = useDebouncedCallback((val: string) => {
    lastSentValue.current = val;
    onChange(val);
  }, debounceDelay);
  const searchRef = useRef<HTMLInputElement>(null);
  const [value, setValue] = useState(defaultValue);
  const metaKey = getMetaKey();
  const { t: translate } = useTranslation(["dags"]);

  useEffect(() => {
    if (defaultValue !== lastSentValue.current) {
      setValue(defaultValue);
      lastSentValue.current = defaultValue;
    }
  }, [defaultValue]);

  const onSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setValue(event.target.value);
    handleSearchChange(event.target.value);
  };
  const clearSearch = () => {
    handleSearchChange.cancel();
    lastSentValue.current = "";
    setValue("");
    onChange("");
  };

  useHotkeys(
    "mod+k",
    () => {
      searchRef.current?.focus();
    },
    { enabled: !hotkeyDisabled, preventDefault: true },
  );

  const inputGroup = (
    <InputGroup
      colorPalette="brand"
      {...props}
      endElement={
        <>
          {Boolean(value) ? (
            <CloseButton
              aria-label={translate("search.clear")}
              colorPalette="brand"
              data-testid="clear-search"
              onClick={clearSearch}
              size="xs"
            />
          ) : undefined}
          {!hotkeyDisabled && (
            <Kbd size="sm">
              {metaKey}
              {translate("search.hotkey")}
            </Kbd>
          )}
        </>
      }
      startElement={<FiSearch />}
    >
      <Input
        data-testid="search-dags"
        onChange={onSearchChange}
        placeholder={placeholder}
        pr={150}
        ref={searchRef}
        value={value}
      />
    </InputGroup>
  );

  if (!advancedSearch) {
    return inputGroup;
  }

  return (
    <HStack alignItems="center" gap={2} w="100%">
      {inputGroup}
      <AdvancedSearchToggle {...advancedSearch} />
    </HStack>
  );
};
