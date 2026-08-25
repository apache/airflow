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
import { Box, HStack, createListCollection } from "@chakra-ui/react";
import { useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { FiClock, FiZap } from "react-icons/fi";

import { Select } from "src/components/ui";

import { FilterPill } from "../FilterPill";
import type { FilterConfig, FilterPluginProps } from "../types";
import { RUN_STATE_LOOKBACKS, isRunStateValue, type RunStateLookback } from "./runStateFilter";

const LOOKBACK_LABEL_KEYS: Record<RunStateLookback, string> = {
  "24": "last24Hours",
  "168": "last7Days",
  "720": "last30Days",
  any: "anyTime",
  latest: "latestRun",
};

type SelectOption = {
  label: string;
  value: string;
};

type RunStateFilterConfig = {
  options: Array<SelectOption>;
};

const collapsePill = () => {
  setTimeout(() => {
    const activeElement = document.activeElement as HTMLElement;

    activeElement.blur();
  }, 0);
};

/**
 * One pill filtering Dags on a run state matched within a lookback: the latest run only, a
 * recent time window, or the whole run history. Committed as a composite value that the
 * config's projections spread over the underlying URL params.
 */
export const RunStateFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation(["common"]);
  const config = filter.config as FilterConfig & RunStateFilterConfig;
  const value = isRunStateValue(filter.value) ? filter.value : undefined;

  // Holds a lookback picked before any state is chosen, when there is nothing to commit yet.
  const [pendingLookback, setPendingLookback] = useState<RunStateLookback>(value?.lookback ?? "latest");

  // Selecting an option closes the menu in the same tick as the value commits, before React has
  // re-rendered. Recording the selection keeps the close from being mistaken for an abandoned
  // filter, which a timing-based check gets wrong.
  const hasJustSelected = useRef(false);

  const handleStateChange = ({ value: selected }: { value: Array<string> }) => {
    const [newState] = selected;

    if (newState === undefined) {
      return;
    }
    hasJustSelected.current = true;
    onChange({ lookback: value?.lookback ?? pendingLookback, state: newState });
    collapsePill();
  };

  const handleLookbackChange = ({ value: selected }: { value: Array<string> }) => {
    const [newLookback] = selected as Array<RunStateLookback>;

    if (newLookback === undefined) {
      return;
    }
    hasJustSelected.current = true;
    setPendingLookback(newLookback);
    if (value !== undefined) {
      onChange({ lookback: newLookback, state: value.state });
      collapsePill();
    }
  };

  const stateCollection = createListCollection({ items: config.options });
  const lookbackCollection = createListCollection({
    items: RUN_STATE_LOOKBACKS.map((lookback) => ({
      label: translate(`common:timeRange.${LOOKBACK_LABEL_KEYS[lookback]}`),
      value: lookback,
    })),
  });

  const stateLabel = config.options.find((option) => option.value === value?.state)?.label;
  const lookbackLabel = lookbackCollection.items.find((item) => item.value === value?.lookback)?.label;

  return (
    <FilterPill
      displayValue={
        value === undefined ? (
          ""
        ) : (
          <HStack display="inline-flex" gap={1}>
            {stateLabel ?? value.state}
            <Box as="span" color="fg.muted" fontSize="xs">
              ({lookbackLabel})
            </Box>
          </HStack>
        )
      }
      filter={filter}
      hasValue={value !== undefined}
      onRemove={onRemove}
      // ``onKeyDown`` is deliberately not forwarded: the selects own Enter and Escape, and letting
      // the pill also act on Enter tore the filter down before the chosen value committed. Escape
      // still closes the pill, through ``onOpenChange`` below.
      renderInput={({ onBlur, onFocus }, { onRequestClose }) => (
        <Box
          alignItems="center"
          bg="bg"
          border="0.5px solid"
          borderColor="border"
          borderRadius="full"
          display="flex"
          h="full"
          onBlur={onBlur}
          onFocus={onFocus}
          overflow="hidden"
          tabIndex={0}
          width="480px"
        >
          <Box
            alignItems="center"
            bg="gray.muted"
            borderLeftRadius="full"
            display="flex"
            fontSize="sm"
            fontWeight="medium"
            h="full"
            px={4}
            py={2}
            whiteSpace="nowrap"
          >
            {filter.config.label}:
          </Box>
          <Select.Root
            border="none"
            collection={stateCollection}
            // A filter added from the menu has nothing to show until it is given a value, so
            // open straight onto the options instead of making the user click again.
            defaultOpen={value === undefined}
            flex="1"
            h="full"
            // Dismissing the menu hands focus back to the trigger inside the pill, so no blur
            // fires and the pill would stay in edit mode. A close that follows a selection is
            // left alone: the blur that selection triggers collapses the pill instead.
            onOpenChange={({ open }) => {
              if (!open && !hasJustSelected.current) {
                onRequestClose();
              }
              hasJustSelected.current = false;
            }}
            onValueChange={handleStateChange}
            value={value === undefined ? [] : [value.state]}
          >
            <Select.Trigger dataTestId={`${filter.config.key}-filter`} triggerProps={{ border: "none" }}>
              <Select.ValueText placeholder={filter.config.placeholder} />
            </Select.Trigger>
            <Select.Content>
              {config.options.map((option) => (
                <Select.Item
                  data-testid={`${filter.config.key}-filter-${option.value}`}
                  item={option}
                  key={option.value}
                >
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <Select.Root
            border="none"
            collection={lookbackCollection}
            h="full"
            onOpenChange={({ open }) => {
              // Unlike the state select, closing this one without picking anything is not an
              // abandoned filter: the state select is the pill's primary control.
              if (!open) {
                hasJustSelected.current = false;
              }
            }}
            onValueChange={handleLookbackChange}
            value={[value?.lookback ?? pendingLookback]}
            width="180px"
          >
            <Select.Trigger dataTestId={`${filter.config.key}-lookback`} triggerProps={{ border: "none" }}>
              <HStack gap={2}>
                {(value?.lookback ?? pendingLookback) === "latest" ? <FiZap /> : <FiClock />}
                <Select.ValueText />
              </HStack>
            </Select.Trigger>
            <Select.Content>
              {lookbackCollection.items.map((item) => (
                <Select.Item
                  data-testid={`${filter.config.key}-lookback-${item.value}`}
                  item={item}
                  key={item.value}
                >
                  <HStack gap={2}>
                    {item.value === "latest" ? <FiZap /> : <FiClock />}
                    {item.label}
                  </HStack>
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>
      )}
    />
  );
};
