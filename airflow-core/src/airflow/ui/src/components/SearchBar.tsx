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
import { Button, Input, Kbd, type ButtonProps } from "@chakra-ui/react";
import { useState, useRef, type ChangeEvent } from "react";
import { useHotkeys } from "react-hotkeys-hook";
import { useTranslation } from "react-i18next";
import { FiSearch } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

import { getMetaKey } from "src/utils";

import { CloseButton, InputGroup, type InputGroupProps } from "./ui";

const debounceDelay = 200;

type Props = {
  readonly buttonProps?: ButtonProps;
  readonly defaultValue: string;
  readonly groupProps?: InputGroupProps;
  readonly hideAdvanced?: boolean;
  readonly hotkeyDisabled?: boolean;
  readonly onChange: (value: string) => void;
  readonly placeHolder: string;
};

export const SearchBar = ({
  buttonProps,
  defaultValue,
  groupProps,
  hideAdvanced = false,
  hotkeyDisabled = false,
  onChange,
  placeHolder,
}: Props) => {
  const handleSearchChange = useDebouncedCallback((val: string) => onChange(val), debounceDelay);
  const searchRef = useRef<HTMLInputElement>(null);
  const [value, setValue] = useState(defaultValue);
  const metaKey = getMetaKey();
  const { t: translate } = useTranslation(["dags"]);
  const onSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setValue(event.target.value);
    handleSearchChange(event.target.value);
  };

  useHotkeys(
    "mod+k",
    () => {
      searchRef.current?.focus();
    },
    { enabled: !hotkeyDisabled, preventDefault: true },
  );

  return (
    <InputGroup
      {...groupProps}
      colorPalette="brand"
      endElement={
        <>
          {Boolean(value) ? (
            <CloseButton
              aria-label={translate("search.clear")}
              colorPalette="brand"
              data-testid="clear-search"
              onClick={() => {
                setValue("");
                onChange("");
              }}
              size="xs"
            />
          ) : undefined}
          {Boolean(hideAdvanced) ? undefined : (
            <Button fontWeight="normal" height="1.75rem" variant="ghost" width={140} {...buttonProps}>
              {translate("search.advanced")}
            </Button>
          )}
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
        placeholder={placeHolder}
        pr={150}
        ref={searchRef}
        value={value}
      />
    </InputGroup>
  );
};
