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
import type { SelectValueChangeDetails } from "@chakra-ui/react";

import { Select } from "src/components/ui";
import { dagSortOptions } from "src/constants/sortParams";

type Props = {
  readonly handleSortChange: ({ value }: SelectValueChangeDetails<Array<string>>) => void;
  readonly orderBy?: string;
};

export const SortSelect = ({ handleSortChange, orderBy }: Props) => (
  <Select.Root
    collection={dagSortOptions}
    data-testid="sort-by-select"
    onValueChange={handleSortChange}
    value={orderBy === undefined ? undefined : [orderBy]}
    width="310px"
  >
    <Select.Trigger>
      <Select.ValueText placeholder="Sort by" />
    </Select.Trigger>
    <Select.Content>
      {dagSortOptions.items.map((option) => (
        <Select.Item item={option} key={option.value}>
          {option.label}
        </Select.Item>
      ))}
    </Select.Content>
  </Select.Root>
);
