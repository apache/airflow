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
import { Field } from "@chakra-ui/react";
import { Select as ReactSelect } from "chakra-react-select";

type FilterOption = {
  label: string;
  value: string;
};

type Props = {
  readonly ariaLabel: string;
  readonly noOptionsMessage: string;
  readonly onChange: (values: Array<string>) => void;
  readonly onInputChange?: (value: string) => void;
  readonly onMenuScrollToBottom?: () => void;
  readonly onMenuScrollToTop?: () => void;
  readonly options: Array<string>;
  readonly placeholder: string;
  readonly values: Array<string>;
};

export const DagsFilterSelect = ({
  ariaLabel,
  noOptionsMessage,
  onChange,
  onInputChange,
  onMenuScrollToBottom,
  onMenuScrollToTop,
  options,
  placeholder,
  values,
}: Props) => (
  <Field.Root>
    <ReactSelect<FilterOption, true>
      aria-label={ariaLabel}
      chakraStyles={{
        clearIndicator: (provided) => ({
          ...provided,
          color: "gray.fg",
        }),
        container: (provided) => ({
          ...provided,
          width: "100%",
        }),
        control: (provided) => ({
          ...provided,
          colorPalette: "brand",
        }),
        menu: (provided) => ({
          ...provided,
          zIndex: 2,
        }),
      }}
      isClearable
      isMulti
      noOptionsMessage={() => noOptionsMessage}
      onChange={(selected) => onChange(selected.map(({ value }) => value))}
      onInputChange={onInputChange}
      onMenuScrollToBottom={onMenuScrollToBottom}
      onMenuScrollToTop={onMenuScrollToTop}
      options={options.map((option) => ({ label: option, value: option }))}
      placeholder={placeholder}
      value={values.map((value) => ({ label: value, value }))}
    />
  </Field.Root>
);
