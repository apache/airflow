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
import { type CollectionItem, createListCollection, type SelectValueChangeDetails } from "@chakra-ui/react";

import { Select } from "src/components/ui";

type Props = {
  readonly handleSelect: (value: CollectionItem) => void;
  readonly placeholderText: string;
  readonly selectedValue: string | undefined;
  readonly values: Array<{ key: string; label: string }> | undefined;
};

export const AttrSelectFilter = ({ handleSelect, placeholderText, selectedValue, values }: Props) => {
  const thingCollection = createListCollection({ items: values ?? [] });
  const handleValueChange = (details: SelectValueChangeDetails) => {
    if (Array.isArray(details.value)) {
      handleSelect(details.value[0]);
    }
  };
  const selectedDisplay = values?.find((item) => item.key === selectedValue);

  return (
    <Select.Root
      collection={thingCollection}
      maxW="200px"
      onValueChange={handleValueChange}
      value={selectedValue === undefined ? [] : [selectedValue]}
    >
      <Select.Trigger colorPalette="blue" minW="max-content">
        <Select.ValueText placeholder={placeholderText} width="auto">
          {() => selectedDisplay?.label}
        </Select.ValueText>
      </Select.Trigger>
      <Select.Content>
        {thingCollection.items.map((option) => (
          <Select.Item item={option.key} key={option.label}>
            {option.label}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
