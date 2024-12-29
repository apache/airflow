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

import { Select } from "src/components/ui";

import type { FlexibleFormElementProps } from ".";

export const isFieldDropdown = (fieldType: string, fieldEnum: Array<string> | null) => {
  const enumTypes = ["string", "number", "integer"];

  return enumTypes.includes(fieldType) && Array.isArray(fieldEnum);
};

const labelLookup = (key: string, valuesDisplay: Record<string, string> | null): string => {
  if (valuesDisplay && typeof valuesDisplay === "object") {
    return valuesDisplay[key] ?? key;
  }

  return key;
};

export const FlexibleFormFieldDropdown = ({ key, param }: FlexibleFormElementProps) => {
  const selectOptions = createListCollection({
    items:
      param.schema.enum?.map((value) => ({
        label: labelLookup(value, param.schema.values_display),
        value,
      })) ?? [],
  });

  // TODO - somehow the dropdown is not working, does not give options :-(
  return (
    <Select.Root
      collection={selectOptions}
      defaultValue={[param.value as string]}
      id={`element_${key}`}
      name={`element_${key}`}
      size="sm"
    >
      <Select.Trigger>
        <Select.ValueText placeholder="Select Value" />
      </Select.Trigger>
      <Select.Content>
        {selectOptions.items.map((option) => (
          <Select.Item item={option} key={option.value}>
            {option.label}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
