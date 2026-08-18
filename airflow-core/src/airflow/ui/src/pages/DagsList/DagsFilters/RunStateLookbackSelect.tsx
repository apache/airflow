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
import { HStack, Text, type Select as ChakraSelect } from "@chakra-ui/react";
import { createListCollection } from "@chakra-ui/react/collection";
import { useTranslation } from "react-i18next";
import { FiClock, FiZap } from "react-icons/fi";

import { Select } from "src/components/ui";

// "latest" matches on the latest run only; numeric values are hours; "any" has no time bound.
export type RunStateLookback = "168" | "24" | "720" | "any" | "latest";

type Props = {
  readonly dataTestId?: string;
  readonly onChange: (value: RunStateLookback) => void;
  readonly triggerProps?: ChakraSelect.TriggerProps;
  readonly value: RunStateLookback;
};

const LOOKBACK_OPTIONS: ReadonlyArray<{ labelKey: string; value: RunStateLookback }> = [
  { labelKey: "latestRun", value: "latest" },
  { labelKey: "last24Hours", value: "24" },
  { labelKey: "last7Days", value: "168" },
  { labelKey: "last30Days", value: "720" },
  { labelKey: "anyTime", value: "any" },
];

export const TIME_LOOKBACKS = LOOKBACK_OPTIONS.map((option) => option.value).filter(
  (value) => value !== "latest" && value !== "any",
);

export const RunStateLookbackSelect = ({ dataTestId, onChange, triggerProps, value }: Props) => {
  const { t: translate } = useTranslation("dags");

  const collection = createListCollection({
    items: LOOKBACK_OPTIONS.map(({ labelKey, value: lookback }) => ({
      label: translate(`common:timeRange.${labelKey}`),
      value: lookback,
    })),
  });

  const current = collection.items.find((item) => item.value === value);

  return (
    <Select.Root
      collection={collection}
      data-testid={dataTestId}
      onValueChange={({ value: selected }) => onChange(selected[0] as RunStateLookback)}
      value={[value]}
      width="fit-content"
    >
      <Select.Trigger triggerProps={triggerProps}>
        <HStack gap={2} justifyContent="space-between" pe={5} width="full">
          <Text color="fg.muted" whiteSpace="nowrap">
            {translate("common:timeRange.in")}:
          </Text>
          <HStack gap={2}>
            {value === "latest" ? <FiZap /> : <FiClock />}
            <Text whiteSpace="nowrap">{current?.label}</Text>
          </HStack>
        </HStack>
      </Select.Trigger>
      <Select.Content>
        {collection.items.map((item) => (
          <Select.Item
            data-testid={dataTestId === undefined ? undefined : `${dataTestId}-${item.value}`}
            item={item}
            key={item.value}
          >
            <HStack gap={2}>
              {item.value === "latest" ? <FiZap /> : <FiClock />}
              <Text>{item.label}</Text>
            </HStack>
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
