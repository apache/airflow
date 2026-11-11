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
import { type SingleValue, Select as ReactSelect } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import type { ParamValue } from "src/queries/useDagParams";
import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";
import { getEnumValueKey, lookupValuesDisplay } from "./getEnumValueKey";

const NULL_STRING_VALUE = "__null__";

const toOptionValue = (value: ParamValue): string =>
  value === null ? NULL_STRING_VALUE : getEnumValueKey(value);

const labelLookup = (key: ParamValue, valuesDisplay: Record<string, string> | undefined): string => {
  const stringKey = key === null ? "null" : getEnumValueKey(key);

  return (
    lookupValuesDisplay(stringKey, valuesDisplay) ?? lookupValuesDisplay("None", valuesDisplay) ?? stringKey
  );
};

export const FieldDropdown = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  const options =
    param.schema.enum?.map((value) => ({
      label: labelLookup(value, param.schema.values_display),
      value: toOptionValue(value),
    })) ?? [];

  const currentValue = options.find((opt) => opt.value === toOptionValue(param.value)) ?? null;

  const handleChange = (
    selected: SingleValue<{
      label: string;
      value: string;
    }>,
  ) => {
    if (paramsDict[name]) {
      if (!selected || selected.value === NULL_STRING_VALUE) {
        paramsDict[name].value = null;
      } else {
        // Map the string value back to the original typed enum value (e.g. number, object)
        // so that backend validation receives the correct type.
        const originalValue = param.schema.enum?.find((enumVal) => toOptionValue(enumVal) === selected.value);

        paramsDict[name].value = originalValue ?? selected.value;
      }
    }

    setParamsDict(paramsDict);
    onUpdate(selected?.value ?? "");
  };

  return (
    <ReactSelect
      id={`element_${name}`}
      isClearable
      isDisabled={disabled}
      menuPosition="fixed"
      name={`element_${name}`}
      onChange={handleChange}
      options={options}
      placeholder={translate("flexibleForm.placeholder")}
      size="sm"
      value={currentValue}
    />
  );
};
