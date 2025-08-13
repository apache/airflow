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

import {Select} from "src/components/ui";
import {type CollectionItem, createListCollection} from "@chakra-ui/react";
import {useTranslation} from "react-i18next";

type Props = {
  readonly allValue: string;
  readonly handleSelect: (value: CollectionItem) => void;
  readonly placeholderText: string;
  readonly values: Array<string>;
  readonly selectedValue: string;
};


export const AttrSelectFilter = ({
  allValue,
  handleSelect,
  placeholderText,
  values,
  selectedValue,
}: Props) => {

  const totalThingList = [ allValue, ...(values ?? [])]
  const thingCollection = createListCollection({items: totalThingList})

  return (
  <Select.Root
    collection={thingCollection}
    maxW="200px"
    onValueChange={handleSelect}
    value={[selectedValue]}
  >
    <Select.Trigger colorPalette="blue" minW="max-content" >
      <Select.ValueText placeholder={placeholderText} width="auto">
        {() => selectedValue}
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
