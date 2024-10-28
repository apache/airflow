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
import {
  Button,
  Input,
  InputGroup,
  InputLeftElement,
  InputRightElement,
  type ButtonProps,
  type InputGroupProps,
  type InputProps,
} from "@chakra-ui/react";
import type { ChangeEvent } from "react";
import { FiSearch } from "react-icons/fi";
import { useDebouncedCallback } from "use-debounce";

const debounceDelay = 200;

export const SearchBar = ({
  buttonProps,
  groupProps,
  inputProps,
}: {
  readonly buttonProps?: ButtonProps;
  readonly groupProps?: InputGroupProps;
  readonly inputProps?: InputProps;
}) => {
  const handleSearchChange = useDebouncedCallback(
    (event: ChangeEvent<HTMLInputElement>) => inputProps?.onChange?.(event),
    debounceDelay,
  );

  return (
    <InputGroup {...groupProps}>
      <InputLeftElement pointerEvents="none">
        <FiSearch />
      </InputLeftElement>
      <Input
        placeholder="Search Dags"
        pr={150}
        {...inputProps}
        onChange={handleSearchChange}
      />
      <InputRightElement width={150}>
        <Button
          colorScheme="blue"
          fontWeight="normal"
          height="1.75rem"
          variant="ghost"
          width={140}
          {...buttonProps}
        >
          Advanced Search
        </Button>
      </InputRightElement>
    </InputGroup>
  );
};
