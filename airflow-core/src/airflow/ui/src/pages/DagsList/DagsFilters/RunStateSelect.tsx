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
import { HStack, Text } from "@chakra-ui/react";
import { createListCollection } from "@chakra-ui/react/collection";
import { useTranslation } from "react-i18next";
import { FiList } from "react-icons/fi";

import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";

type RunState = "failed" | "queued" | "running" | "success";

type Props = {
  readonly dataTestId?: string;
  readonly label: string;
  readonly onChange: (value: string | undefined) => void;
  readonly states: ReadonlyArray<RunState>;
  readonly value: string | undefined;
};

const ALL_VALUE = "all";

export const RunStateSelect = ({ dataTestId, label, onChange, states, value }: Props) => {
  const { t: translate } = useTranslation(["dags", "common"]);

  const collection = createListCollection({
    items: [
      { label: translate("dags:filters.allStates"), state: undefined, value: ALL_VALUE },
      ...states.map((state) => ({ label: translate(`common:states.${state}`), state, value: state })),
    ],
  });

  const current = collection.items.find((item) => item.value === (value ?? ALL_VALUE));

  return (
    <Select.Root
      collection={collection}
      data-testid={dataTestId}
      minWidth="232px"
      onValueChange={({ value: selected }) => onChange(selected[0] === ALL_VALUE ? undefined : selected[0])}
      value={[value ?? ALL_VALUE]}
      width="fit-content"
    >
      <Select.Trigger>
        <HStack gap={2} justifyContent="space-between" pe={5} width="full">
          <Text color="fg.muted" whiteSpace="nowrap">
            {label}:
          </Text>
          <HStack gap={2}>
            {current?.state === undefined ? <FiList /> : <StateBadge state={current.state} />}
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
              {item.state === undefined ? <FiList /> : <StateBadge state={item.state} />}
              <Text>{item.label}</Text>
            </HStack>
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
