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
import { HStack, type SelectValueChangeDetails, type ListCollection } from "@chakra-ui/react";
import { Select } from "src/components/ui";
import { StateBadge } from "src/components/StateBadge";
import type { TaskInstanceState } from "openapi/requests/types.gen";
import { taskInstanceStateOptions } from "src/constants/stateOptions";

type Option<V extends string> = { label: string; value: V };

type Props<V extends string> = {
  value: V[]; // e.g. (TaskInstanceState | "all" | "none")[]
  onChange: (details: SelectValueChangeDetails<V>) => void;
  t: (key: string) => string;
  maxW?: string;
};

export function StateFilter<V extends string>({
  value,
  onChange,
  t,
  maxW = "450px",
}: Props<V>) {
  const hasFilteredState = !(value.length === 0 || (value.length === 1 && (value[0] as unknown as string) === "all"));

  return (
    <Select.Root
      collection={taskInstanceStateOptions}
      maxW={maxW}
      multiple
      onValueChange={onChange}
      value={hasFilteredState ? value : (["all"] as unknown as V[])}
    >
      <Select.Trigger {...(hasFilteredState ? { clearable: true } : {})} colorPalette="brand" isActive={hasFilteredState}>
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
                    {t(`common:states.${state as string}`)}
                  </StateBadge>
                ))}
              </HStack>
            ) : (
              t("dags:filters.allStates")
            )
          }
        </Select.ValueText>
      </Select.Trigger>

      <Select.Content>
        {taskInstanceStateOptions.items.map((option) => (
          <Select.Item item={option} key={option.label}>
            {(option.value as unknown as string) === "all"
              ? t(option.label)
              : (
                <StateBadge state={option.value as unknown as TaskInstanceState}>
                  {t(option.label)}
                </StateBadge>
              )}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
}