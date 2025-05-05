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
import { type MultiValue, Select as ReactSelect } from "chakra-react-select";
import { useState } from "react";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

const labelLookup = (key: string, valuesDisplay: Record<string, string> | undefined): string => {
  if (valuesDisplay && typeof valuesDisplay === "object") {
    return valuesDisplay[key] ?? key;
  }

  return key;
};

export const FieldMultiSelect = ({ name, onUpdate }: FlexibleFormElementProps) => {
  const { paramsDict, setParamsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;

  // Initialize `selectedOptions` directly from `paramsDict`
  const [selectedOptions, setSelectedOptions] = useState(
    Array.isArray(param.value)
      ? (param.value as Array<string>).map((value) => ({
          label: labelLookup(value, param.schema.values_display),
          value,
        }))
      : [],
  );

  // Handle changes to the select field
  const handleChange = (
    newValue: MultiValue<{
      label: string;
      value: string;
    }>,
  ) => {
    const updatedOptions = [...newValue];

    setSelectedOptions(updatedOptions);

    // "undefined" values are removed from params, so we set it to null to avoid falling back to DAG defaults.
    // eslint-disable-next-line unicorn/no-null
    const newValueArray = updatedOptions.length ? updatedOptions.map((option) => option.value) : null;

    if (paramsDict[name]) {
      paramsDict[name].value = newValueArray;
    }
    setParamsDict(paramsDict);
    onUpdate(String(newValueArray));
  };

  return (
    <ReactSelect
      aria-label="Select one or multiple values"
      id={`element_${name}`}
      isClearable
      isMulti
      name={`element_${name}`}
      onChange={handleChange}
      options={
        param.schema.examples?.map((value) => ({
          label: labelLookup(value, param.schema.values_display),
          value,
        })) ?? []
      }
      size="sm"
      value={selectedOptions}
    />
  );
};
