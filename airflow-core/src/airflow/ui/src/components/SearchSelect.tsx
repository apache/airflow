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
import { type ReactNode, createContext, use } from "react";

import { Box, Field } from "@chakra-ui/react";
import type { ChakraStylesConfig, ControlProps, GroupBase, SingleValue } from "chakra-react-select";
import { Select, chakraComponents } from "chakra-react-select";
import { useTranslation } from "react-i18next";
import { FiSearch } from "react-icons/fi";

// react-select builds its components from a stable map, so anything a caller wants inside the
// control has to reach it some other way than a prop: rebuilding the control to close over one
// would remount the input on every render and drop focus mid-keystroke.
const ControlEndContext = createContext<ReactNode>(undefined);

/**
 * Leads the input with the search affordance and closes it with whatever the caller put at the end.
 * react-select only renders indicators after the value container, so an icon on the start side has
 * to come from the control itself.
 */
const SearchControl = <Option,>({ children, ...props }: ControlProps<Option, false>) => (
  <chakraComponents.Control {...props}>
    <Box alignItems="center" as="span" color="fg.muted" display="flex" flexShrink={0} pe={1.5}>
      <FiSearch />
    </Box>
    {children}
    {use(ControlEndContext)}
  </chakraComponents.Control>
);

/** The server decides what matches, so react-select must not filter the answer again. */
const keepEveryOption = () => true;

type Props<Option> = {
  /** Rendered at the end of the input, where an addon sits on a search field elsewhere. */
  readonly controlEnd?: ReactNode;
  readonly formatOptionLabel: (option: Option) => ReactNode;
  readonly inputValue: string;
  readonly onChange: (selected: SingleValue<Option>) => void;
  readonly onInputChange: (value: string) => void;
  readonly options: Array<Option>;
  readonly placeholder: string;
};

/**
 * The search a breadcrumb switcher drops open.
 *
 * The caller owns the input and what is listed for it, rather than react-select's async mode
 * owning both: that only reloads from its own input handler, so a search could not be re-run when
 * something other than the text changed it — such as turning match-anywhere on. Holding the text
 * here also means the options on screen are whatever the caller last had, so a search in flight
 * leaves the previous answer up instead of emptying the panel.
 */
export const SearchSelect = <Option,>({
  controlEnd,
  formatOptionLabel,
  inputValue,
  onChange,
  onInputChange,
  options,
  placeholder,
}: Props<Option>) => {
  const { t: translate } = useTranslation();
  // The popover is the card. Drop the floating menu's own positioning and chrome so the results
  // flow inside it directly under the input, instead of reading as a second card.
  const chakraStyles: ChakraStylesConfig<Option, false, GroupBase<Option>> = {
    // Whatever the caller puts at the end of the control is an addon: it fills the control's
    // height and reaches its edge, so the control itself gives up its end padding.
    control: (provided) => ({ ...provided, overflow: "hidden", paddingInlineEnd: 0 }),
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
    <ControlEndContext value={controlEnd}>
      <Field.Root>
        <Select<Option>
          backspaceRemovesValue={true}
          chakraStyles={chakraStyles}
          components={{ Control: SearchControl, DropdownIndicator: null }}
          filterOption={keepEveryOption}
          formatOptionLabel={formatOptionLabel}
          inputValue={inputValue}
          menuIsOpen
          noOptionsMessage={() => translate("table.noResultsFound")}
          onChange={onChange}
          // Only a keystroke is a new search; react-select also reports blur and menu close here,
          // which would otherwise wipe the text while the panel is still open.
          onInputChange={(value, { action }) => {
            if (action === "input-change") {
              onInputChange(value);
            }
          }}
          options={options}
          placeholder={placeholder}
          value={null} // null is required https://github.com/JedWatson/react-select/issues/3066
        />
      </Field.Root>
    </ControlEndContext>
  );
};
