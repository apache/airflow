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

import { Select } from "src/components/ui";

import type { FlexibleFormElementProps } from ".";

const labelLookup = (key: string, valuesDisplay: Record<string, string> | undefined): string => {
  if (valuesDisplay && typeof valuesDisplay === "object") {
    return valuesDisplay[key] ?? key;
  }

  return key;
};
const enumTypes = ["string", "number", "integer"];

export const FieldDropdown = ({ name, param }: FlexibleFormElementProps) => {
  const selectOptions = createListCollection({
    items:
      param.schema.enum?.map((value) => ({
        label: labelLookup(value, param.schema.values_display),
        value,
      })) ?? [],
  });
  const contentRef = useRef<HTMLDivElement>(null);

  return (
    <Select.Root
      collection={selectOptions}
      defaultValue={enumTypes.includes(typeof param.value) ? [String(param.value)] : undefined}
      id={`element_${name}`}
      name={`element_${name}`}
      ref={contentRef}
      size="sm"
    >
      <Select.Trigger>
        <Select.ValueText placeholder="Select Value" />
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
