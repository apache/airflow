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
import { Box, Icon, Input, InputGroup, type InputGroupProps } from "@chakra-ui/react";
import { useEffect, useRef, useState, type ChangeEvent } from "react";
import { useTranslation } from "react-i18next";
import { FiSearch, FiX } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

import { AdvancedSearchToggle, type AdvancedSearchToggleProps } from "src/components/AdvancedSearchToggle";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";
import { getMetaKey } from "src/utils";

import { IconButton } from "./ui";

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

  useShortcut({
    ...SHORTCUTS.search.focusSearch,
    callback: () => {
      searchRef.current?.focus();
    },
    options: { enabled: !hotkeyDisabled, preventDefault: true },
  });

  return (
    <InputGroup
      colorPalette="brand"
      {...props}
      endElement={
        Boolean(value) || advancedSearch ? (
          <Box alignItems="center" bg="bg" display="flex" gap={1} mr={-2}>
            {Boolean(value) ? (
              <IconButton
                data-testid="clear-search"
                label={translate("search.clear")}
                onClick={clearSearch}
                size="xs"
                variant="ghost"
              >
                <FiX />
              </IconButton>
            ) : undefined}
            {advancedSearch ? <AdvancedSearchToggle size="xs" {...advancedSearch} /> : undefined}
          </Box>
        ) : undefined
      }
      startElement={<Icon as={FiSearch} color="fg.subtle" />}
    >
      <Input
        data-testid="search-dags"
        onChange={onSearchChange}
        placeholder={
          hotkeyDisabled ? placeholder : `${placeholder} (${metaKey}${translate("search.hotkey")})`
        }
        ref={searchRef}
        value={value}
      />
    </InputGroup>
  );
};
