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
import { type CollectionItem, createListCollection } from "@chakra-ui/react";
import type { SelectValueChangeDetails } from "@chakra-ui/react";

import { Select } from "src/components/ui";

type Props = {
  readonly displayPrefix: string | undefined;
  readonly handleSelect: (values: Array<CollectionItem>) => void;
  readonly placeholderText: string;
  readonly selectedValues: Array<string> | undefined;
  readonly values: Array<string> | undefined;
};

export const AttrSelectFilterMulti = ({
  displayPrefix,
  handleSelect,
  placeholderText,
  selectedValues,
  values,
}: Props) => {
  const thingCollection = createListCollection({ items: values ?? [] });

  const handleValueChange = (details: SelectValueChangeDetails) => {
    if (Array.isArray(details.value)) {
      handleSelect(details.value);
    }
  };
  let displayValue = selectedValues?.join(", ") ?? undefined;

  if (displayValue !== undefined && displayPrefix !== undefined) {
    displayValue = `${displayPrefix}: ${displayValue}`;
  }

  // debugger;
  return (
    <Select.Root
      collection={thingCollection}
      maxW="200px"
      multiple
      onValueChange={handleValueChange}
      value={selectedValues}
    >
      <Select.Trigger colorPalette="brand" minW="max-content">
        <Select.ValueText placeholder={placeholderText} width="auto">
          {() => displayValue}
        </Select.ValueText>
      </Select.Trigger>
      <Select.Content>
        {thingCollection.items.map((option) => (
          <Select.Item item={option} key={option}>
            {option}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
