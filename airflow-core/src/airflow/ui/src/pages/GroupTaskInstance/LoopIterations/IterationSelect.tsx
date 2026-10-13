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
import { useEffect } from "react";

import { createListCollection, HStack, Text, VStack } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";

import type { LoopSummaryResponse } from "openapi/requests/types.gen";

import { Select } from "src/system-components";

import { StateBadge } from "src/components/StateBadge";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useDurationFormat } from "src/utils";

import { getSelectedIteration, ITERATION_ALL } from "./loopUtils";

type Props = {
  readonly summary: LoopSummaryResponse;
};

export const IterationSelect = ({ summary }: Props) => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const [searchParams, setSearchParams] = useSearchParams();
  const { formatElapsed } = useDurationFormat();
  const param = searchParams.get(SearchParamsKeys.ITERATION);
  const selected = getSelectedIteration(param, summary);

  // Only a stale iteration (e.g. cleared away) is rewritten; its cursor paged a list that no longer exists.
  useEffect(() => {
    const regionId = searchParams.get(SearchParamsKeys.LOOP_REGION_ID);

    if (param === null || param === selected || (regionId !== null && regionId !== summary.loop_region_id)) {
      return;
    }
    const next = new URLSearchParams(searchParams);

    next.set(SearchParamsKeys.ITERATION, selected);
    next.delete(SearchParamsKeys.CURSOR);
    setSearchParams(next, { replace: true });
  }, [param, searchParams, selected, setSearchParams, summary.loop_region_id]);

  const options = [
    { detail: undefined, label: translate("loop.filter.all"), state: undefined, value: ITERATION_ALL },
    ...summary.iterations.map((iteration) => ({
      detail: formatElapsed(iteration.start_date, iteration.end_date),
      label: translate("loop.iteration", { index: iteration.index }),
      state: iteration.state,
      value: String(iteration.index),
    })),
  ];
  const collection = createListCollection({ items: options });
  const invocations = createListCollection({
    items: (summary.loop_regions ?? []).map((region, index) => ({
      label: `${summary.group_id} (${index + 1})`,
      value: region.region_id,
    })),
  });

  return (
    <HStack gap={2}>
      {invocations.items.length > 1 ? (
        <Select.Root
          collection={invocations}
          onValueChange={({ value }) => {
            const params = new URLSearchParams(searchParams);
            const [regionId] = value;

            if (regionId === undefined) {
              return;
            }
            params.set(SearchParamsKeys.LOOP_REGION_ID, regionId);
            params.delete(SearchParamsKeys.ITERATION);
            params.delete(SearchParamsKeys.CURSOR);
            setSearchParams(params);
          }}
          value={
            summary.loop_region_id === null || summary.loop_region_id === undefined
              ? []
              : [summary.loop_region_id]
          }
          width="260px"
        >
          <Select.Trigger>
            <Select.ValueText placeholder={translate("loop.filter.invocation")} />
          </Select.Trigger>
          <Select.Content>
            {invocations.items.map((item) => (
              <Select.Item item={item} key={item.value}>
                {item.label}
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
      ) : undefined}
      <Text color="fg.muted" fontSize="sm" whiteSpace="nowrap">
        {translate("loop.filter.label")}
      </Text>
      <Select.Root
        collection={collection}
        onValueChange={(event) => {
          const [next] = event.value;
          const params = new URLSearchParams(searchParams);

          params.set(SearchParamsKeys.ITERATION, next ?? ITERATION_ALL);
          params.delete(SearchParamsKeys.CURSOR);
          setSearchParams(params);
        }}
        value={[selected]}
        width="260px"
      >
        <Select.Trigger dataTestId="loop-iteration-select">
          <Select.ValueText />
        </Select.Trigger>
        <Select.Content>
          {options.map((option) => (
            <Select.Item item={option} key={option.value}>
              <VStack align="start" gap={0}>
                <HStack gap={2}>
                  <Text>{option.label}</Text>
                  {option.state === null || option.state === undefined ? undefined : (
                    <StateBadge state={option.state}>{translate(`common:states.${option.state}`)}</StateBadge>
                  )}
                </HStack>
                {option.detail === undefined || option.detail === "" ? undefined : (
                  <Text color="fg.muted" fontSize="xs">
                    {option.detail}
                  </Text>
                )}
              </VStack>
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Root>
    </HStack>
  );
};
