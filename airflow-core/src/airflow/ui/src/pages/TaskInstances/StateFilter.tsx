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
import { HStack, type SelectValueChangeDetails } from "@chakra-ui/react";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { taskInstanceStateOptions } from "src/constants/stateOptions";

type Props<V extends string> = {
  readonly maxW?: string;
  readonly onChange: (details: SelectValueChangeDetails<V>) => void;
  readonly translate: (key: string) => string;
  readonly value: Array<V>; // e.g. (TaskInstanceState | "all" | "none")[]
};

export const StateFilter = <V extends string>({ maxW = "450px", onChange, translate, value }: Props<V>) => {
  const hasFilteredState = !(
    value.length === 0 ||
    (value.length === 1 && (value[0] as unknown as string) === "all")
  );

  return (
    <Select.Root
      collection={taskInstanceStateOptions}
      maxW={maxW}
      multiple
      onValueChange={onChange}
      value={hasFilteredState ? value : (["all"] as unknown as Array<V>)}
    >
      <Select.Trigger
        {...(hasFilteredState ? { clearable: true } : {})}
        colorPalette="brand"
        isActive={hasFilteredState}
      >
        <Select.ValueText>
          {() =>
            hasFilteredState ? (
              <HStack flexWrap="wrap" fontSize="sm" gap="4px" paddingY="8px">
                {value.map((state) => (
                  <StateBadge
                    key={state as string}
                    // safe because when hasFilteredState is true, we never include "all"/"none"
                    state={state as unknown as TaskInstanceState}
                  >
                    {translate(`common:states.${state as string}`)}
                  </StateBadge>
                ))}
              </HStack>
            ) : (
              translate("dags:filters.allStates")
            )
          }
        </Select.ValueText>
      </Select.Trigger>

      <Select.Content>
        {taskInstanceStateOptions.items.map((option) => (
          <Select.Item item={option} key={option.label}>
            {(option.value as unknown as string) === "all" ? (
              translate(option.label)
            ) : (
              <StateBadge state={option.value as unknown as TaskInstanceState}>
                {translate(option.label)}
              </StateBadge>
            )}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
