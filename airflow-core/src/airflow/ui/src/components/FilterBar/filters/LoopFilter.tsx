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
import { useRef } from "react";

import { Box, HStack, createListCollection } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { MdLoop } from "react-icons/md";
import { useParams } from "react-router-dom";

import { Select } from "src/system-components";

import { LoopOptionLabel } from "src/components/LoopOptionLabel";

import {
  decodeLoopOption,
  encodeLoopOption,
  useLoopFilterOptions,
  type LoopOption,
} from "src/queries/useLoopFilterOptions";

import { FilterPill } from "../FilterPill";
import type { FilterPluginProps } from "../types";
import { isLoopFilterValue } from "./loopParams";

const collapsePill = () => {
  setTimeout(() => {
    const activeElement = document.activeElement as HTMLElement;

    activeElement.blur();
  }, 0);
};

/**
 * One pill, one list: every loop in the run and the passes it ran, as single choices.
 *
 * A pass is only ever offered under the loop that counts it, so the two halves cannot be set
 * independently -- an iteration without a loop, or one the loop never reached, both used to
 * leave the table empty with nothing to say why.
 */
export const LoopFilter = ({ filter, onChange, onRemove }: FilterPluginProps) => {
  const { t: translate } = useTranslation(["common"]);
  const { dagId, groupId, runId } = useParams();
  const value = isLoopFilterValue(filter.value) ? filter.value : undefined;
  const { options } = useLoopFilterOptions({ dagId, groupId, runId });

  const hasJustSelected = useRef(false);
  const selectedValue =
    value === undefined
      ? undefined
      : encodeLoopOption(value.loopId, value.iteration === undefined ? undefined : Number(value.iteration));

  const items = options.map((option) => ({ ...option, label: `${option.loopId} ${option.iteration ?? ""}` }));
  const collection = createListCollection({ items });
  const current = options.find((option) => option.value === selectedValue);

  const handleChange = ({ value: picked }: { value: Array<string> }) => {
    const [choice] = picked;

    if (choice === undefined) {
      return;
    }
    hasJustSelected.current = true;
    onChange(decodeLoopOption(choice));
    collapsePill();
  };

  return (
    <FilterPill
      displayValue={current === undefined ? "" : <LoopOptionLabel compact option={current} />}
      filter={filter}
      hasValue={value !== undefined}
      onRemove={onRemove}
      // ``onKeyDown`` is deliberately not forwarded, for the reason given on RunStateFilter: the
      // select owns Enter and Escape, and the pill acting on Enter too tears the filter down
      // before the chosen value commits.
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
            collection={collection}
            // A filter added from the menu has nothing to show until it is given a value, so
            // open straight onto the options instead of making the user click again.
            defaultOpen={value === undefined}
            flex="1"
            h="full"
            onOpenChange={({ open }) => {
              if (!open && !hasJustSelected.current) {
                onRequestClose();
              }
              hasJustSelected.current = false;
            }}
            onValueChange={handleChange}
            value={selectedValue === undefined ? [] : [selectedValue]}
          >
            <Select.Trigger dataTestId={`${filter.config.key}-filter`} triggerProps={{ border: "none" }}>
              <HStack gap={2}>
                <MdLoop />
                <Select.ValueText placeholder={filter.config.placeholder} />
              </HStack>
            </Select.Trigger>
            <Select.Content>
              {items.map((item) => (
                <Select.Item
                  data-testid={`${filter.config.key}-filter-${item.value}`}
                  item={item}
                  key={item.value}
                >
                  <LoopOptionLabel option={item} />
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </Box>
      )}
    />
  );
};
