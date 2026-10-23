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
import { useEffect } from "react";

import { type SingleValue, Select as ReactSelect } from "chakra-react-select";
import { useTranslation } from "react-i18next";

import { paramPlaceholder, useParamStore } from "src/queries/useParamStore";

import type { FlexibleFormElementProps } from ".";

const NULL_STRING_VALUE = "__null__";

const labelLookup = (
  key: boolean | number | string | null,
  valuesDisplay: Record<string, string> | undefined,
): string => {
  if (valuesDisplay && typeof valuesDisplay === "object") {
    const stringKey = key === null ? "null" : String(key);

    return valuesDisplay[stringKey] ?? valuesDisplay.None ?? stringKey;
  }

  return key === null ? "null" : String(key);
};

const enumTypes = ["string", "number", "integer"];

// Python str() of a string, integer, boolean or null parent value, because the serialized Dag
// stores the keys of options_map with str()
const getOptionKey = (value: unknown): string => {
  if (value === null || value === undefined) {
    return "None";
  }
  if (typeof value === "boolean") {
    return value ? "True" : "False";
  }

  return typeof value === "number" || typeof value === "string" ? String(value) : JSON.stringify(value);
};

export const FieldDropdown = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  const { depends_on: dependsOn, options_map: optionsMap } = param.schema;
  const enumValues =
    dependsOn === undefined
      ? param.schema.enum
      : (optionsMap?.[getOptionKey(paramsDict[dependsOn]?.value)] ?? []);

  const options =
    enumValues?.map((value) => ({
      label: labelLookup(value, param.schema.values_display),
      value: String(value ?? NULL_STRING_VALUE),
    })) ?? [];

  const currentValue =
    param.value === null
      ? (options.find((opt) => opt.value === NULL_STRING_VALUE) ?? null)
      : enumTypes.includes(typeof param.value)
        ? (options.find((opt) => opt.value === String(param.value)) ?? null)
        : null;

  // A parent change can leave the value outside the new options, so clear it
  const isStale = dependsOn !== undefined && param.value !== null && currentValue === null;

  useEffect(() => {
    const current = paramsDict[name];

    if (isStale && current) {
      setParamsDict({ ...paramsDict, [name]: { ...current, value: null } });
      onUpdate("");
    }
  }, [isStale, name, onUpdate, paramsDict, setParamsDict]);

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
        // Map the string value back to the original typed enum value (e.g. number, string)
        // so that backend validation receives the correct type.
        const originalValue = enumValues?.find(
          (enumVal) => String(enumVal ?? NULL_STRING_VALUE) === selected.value,
        );

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
