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
import { Select as ReactSelect } from "chakra-react-select";
import { useEffect, useState } from "react";

import type { FlexibleFormElementProps } from ".";
import { paramPlaceholder, useParamStore } from "../TriggerDag/useParamStore";

const labelLookup = (key: string, valuesDisplay: Record<string, string> | undefined): string => {
  if (valuesDisplay && typeof valuesDisplay === "object") {
    return valuesDisplay[key] ?? key;
  }

  return key;
};

export const FieldMultiSelect = ({ name }: FlexibleFormElementProps) => {
  const { paramsDict, setParamsDict } = useParamStore();
  const param = paramsDict[name] ?? paramPlaceholder;
  const [selectedOptions, setSelectedOptions] = useState(
    Array.isArray(param.value)
      ? (param.value as Array<string>).map((value) => ({
          label: labelLookup(value, param.schema.values_display),
          value,
        }))
      : [],
  );

  useEffect(() => {
    if (paramsDict[name]) {
      paramsDict[name].value = selectedOptions.map((option) => option.value);
    }
    setParamsDict(paramsDict);
  }, [selectedOptions, paramsDict, setParamsDict, name]);

  return (
    <ReactSelect
      aria-label="Select one or multiple values"
      id={`element_${name}`}
      isClearable
      isMulti
      name={`element_${name}`}
      onChange={(newValue) => setSelectedOptions([...newValue])}
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
