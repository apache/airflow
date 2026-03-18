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
import { createListCollection } from "@chakra-ui/react/collection";
import { useRef } from "react";
import { useTranslation } from "react-i18next";

import { Select } from "src/components/ui";
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

export const FieldDropdown = ({ name, namespace = "default", onUpdate }: FlexibleFormElementProps) => {
  const { t: translate } = useTranslation("components");
  const { disabled, paramsDict, setParamsDict } = useParamStore(namespace);
  const param = paramsDict[name] ?? paramPlaceholder;

  const selectOptions = createListCollection({
    items:
      param.schema.enum?.map((value) => {
        // Convert null to string constant for zag-js compatibility
        const stringValue = String(value ?? NULL_STRING_VALUE);

        return {
          label: labelLookup(value, param.schema.values_display),
          value: stringValue,
        };
      }) ?? [],
  });

  const contentRef = useRef<HTMLDivElement | null>(null);

  const handleChange = ([value]: Array<string>) => {
    if (paramsDict[name]) {
      if (value === NULL_STRING_VALUE || value === undefined) {
        // eslint-disable-next-line unicorn/no-null
        paramsDict[name].value = null;
      } else {
        // Map the string value back to the original typed enum value (e.g. number, string)
        // so that backend validation receives the correct type.
        const originalValue = param.schema.enum?.find(
          (enumVal) => String(enumVal ?? NULL_STRING_VALUE) === value,
        );

        paramsDict[name].value = originalValue ?? value;
      }
    }

    setParamsDict(paramsDict);
    onUpdate(value);
  };

  return (
    <Select.Root
      collection={selectOptions}
      disabled={disabled}
      id={`element_${name}`}
      name={`element_${name}`}
      onValueChange={(event) => handleChange(event.value)}
      ref={contentRef}
      size="sm"
      value={
        param.value === null
          ? [NULL_STRING_VALUE]
          : enumTypes.includes(typeof param.value)
            ? [String(param.value as number | string)]
            : undefined
      }
    >
      <Select.Trigger clearable>
        <Select.ValueText placeholder={translate("flexibleForm.placeholder")} />
      </Select.Trigger>
      <Select.Content portalRef={contentRef}>
        {selectOptions.items.map((option) => (
          <Select.Item item={option} key={option.value}>
            {option.label}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
