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
import type { ReactNode } from "react";

import { Box, Field } from "@chakra-ui/react";
import type {
  ChakraStylesConfig,
  ControlProps,
  GroupBase,
  OptionsOrGroups,
  SingleValue,
} from "chakra-react-select";
import { AsyncSelect, chakraComponents } from "chakra-react-select";
import { FiSearch } from "react-icons/fi";

/**
 * Leads the input with the search affordance. react-select only renders indicators after the value
 * container, so an icon on the start side has to come from the control itself.
 */
const SearchControl = <Option,>({ children, ...props }: ControlProps<Option, false>) => (
  <chakraComponents.Control {...props}>
    <Box alignItems="center" as="span" color="fg.muted" display="flex" flexShrink={0} pe={1.5}>
      <FiSearch />
    </Box>
    {children}
  </chakraComponents.Control>
);

type Props<Option> = {
  /**
   * The options to list before anything is typed. Always an array, never `true`: `true` makes
   * react-select call `loadOptions` from a mount effect and start out `isLoading`, so every open
   * of a panel that unmounts on close is a spinner over an empty list. A fresh array replaces what
   * is listed, which is how a live query keeps an open panel current.
   */
  readonly defaultOptions: Array<Option>;
  readonly formatOptionLabel: (option: Option) => ReactNode;
  readonly isLoading?: boolean;
  readonly loadOptions: (
    inputValue: string,
    callback: (options: OptionsOrGroups<Option, GroupBase<Option>>) => void,
  ) => void;
  readonly onChange: (selected: SingleValue<Option>) => void;
  readonly placeholder: string;
};

/**
 * The search a breadcrumb switcher drops open. Its results are listed from the moment it mounts,
 * since the panel exists only to show them.
 */
export const SearchSelect = <Option,>({
  defaultOptions,
  formatOptionLabel,
  isLoading,
  loadOptions,
  onChange,
  placeholder,
}: Props<Option>) => {
  // The popover is the card. Drop the floating menu's own positioning and chrome so the results
  // flow inside it directly under the input, instead of reading as a second card.
  const chakraStyles: ChakraStylesConfig<Option, false, GroupBase<Option>> = {
    menu: () => ({ marginTop: 2, width: "100%" }),
    menuList: (provided) => ({
      ...provided,
      background: "transparent",
      borderRadius: 0,
      boxShadow: "none",
      paddingInline: 0,
      zIndex: "auto",
    }),
  };

  return (
    <Field.Root>
      <AsyncSelect<Option>
        backspaceRemovesValue={true}
        chakraStyles={chakraStyles}
        components={{ Control: SearchControl, DropdownIndicator: null }}
        defaultOptions={defaultOptions}
        filterOption={undefined}
        formatOptionLabel={formatOptionLabel}
        isLoading={isLoading}
        loadOptions={loadOptions}
        menuIsOpen
        onChange={onChange}
        placeholder={placeholder}
        value={null} // null is required https://github.com/JedWatson/react-select/issues/3066
      />
    </Field.Root>
  );
};
