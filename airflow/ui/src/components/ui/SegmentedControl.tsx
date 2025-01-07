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
import { Tabs, For, type TabsRootProps } from "@chakra-ui/react";

type Option = {
  disabled?: boolean;
  label: string;
  value: string;
};

type SegmentedControlProps = {
  readonly onValueChange: (value: string) => void;
  readonly options: Array<Option>;
  readonly value: string;
} & Omit<TabsRootProps, "onValueChange">;

const SegmentedControl = ({ onValueChange, options, value, ...rest }: SegmentedControlProps) => (
  <Tabs.Root
    defaultValue={value}
    onValueChange={(option) => onValueChange(option.value)}
    variant="enclosed"
    {...rest}
  >
    <Tabs.List>
      <For each={options}>
        {(option) => (
          <Tabs.Trigger disabled={option.disabled} key={option.value} value={option.value}>
            {option.label}
          </Tabs.Trigger>
        )}
      </For>
    </Tabs.List>
  </Tabs.Root>
);

export default SegmentedControl;
