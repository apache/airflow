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
import { forwardRef } from "react";
import {
  Button,
  type ButtonProps,
  Input,
  InputGroup,
  type InputGroupProps,
  InputLeftElement,
  type InputProps,
  InputRightElement,
} from "@chakra-ui/react";
import { FiSearch } from "react-icons/fi";

export const SearchBar = forwardRef<HTMLInputElement, {
  buttonProps?: ButtonProps;
  groupProps?: InputGroupProps;
  inputProps?: InputProps;
}>(({ buttonProps, groupProps, inputProps }, ref) => (
  <InputGroup {...groupProps}>
    <InputLeftElement pointerEvents="none">
      <FiSearch />
    </InputLeftElement>
    <Input placeholder="Search DAGs" pr={150} {...inputProps} ref={ref} />
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
));
