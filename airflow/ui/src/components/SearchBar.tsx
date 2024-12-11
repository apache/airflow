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
import { Button, Input, type ButtonProps } from "@chakra-ui/react";
import { useState, type ChangeEvent } from "react";
import { FiSearch } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

import { CloseButton, InputGroup, type InputGroupProps } from "./ui";

const debounceDelay = 200;

type Props = {
  readonly buttonProps?: ButtonProps;
  readonly defaultValue: string;
  readonly groupProps?: InputGroupProps;
  readonly onChange: (value: string) => void;
  readonly placeHolder: string;
};

export const SearchBar = ({
  buttonProps,
  defaultValue,
  groupProps,
  onChange,
  placeHolder,
}: Props) => {
  const handleSearchChange = useDebouncedCallback(
    (val: string) => onChange(val),
    debounceDelay,
  );

  const [value, setValue] = useState(defaultValue);

  const onSearchChange = (event: ChangeEvent<HTMLInputElement>) => {
    setValue(event.target.value);
    handleSearchChange(event.target.value);
  };

  return (
    <InputGroup
      {...groupProps}
      colorPalette="blue"
      endElement={
        <>
          {Boolean(value) ? (
            <CloseButton
              aria-label="Clear search"
              colorPalette="gray"
              data-testid="clear-search"
              onClick={() => {
                setValue("");
                onChange("");
              }}
              size="xs"
            />
          ) : undefined}
          <Button
            fontWeight="normal"
            height="1.75rem"
            variant="ghost"
            width={140}
            {...buttonProps}
          >
            Advanced Search
          </Button>
        </>
      }
      startElement={<FiSearch />}
    >
      <Input
        data-testid="search-dags"
        onChange={onSearchChange}
        placeholder={placeHolder}
        pr={150}
        value={value}
      />
    </InputGroup>
  );
};
