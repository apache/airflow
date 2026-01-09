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
import { Button, HStack } from "@chakra-ui/react";
import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { MdAdd, MdClear } from "react-icons/md";
import { useDebouncedCallback } from "use-debounce";

import { Menu } from "src/components/ui";

import { getDefaultFilterIcon } from "./defaultIcons";
import { DateFilter } from "./filters/DateFilter";
import { NumberFilter } from "./filters/NumberFilter";
import { SelectFilter } from "./filters/SelectFilter";
import { TextSearchFilter } from "./filters/TextSearchFilter";
import type { FilterBarProps, FilterConfig, FilterState, FilterValue } from "./types";

const defaultInitialValues: Record<string, FilterValue> = {};

const getFilterIcon = (config: FilterConfig) => config.icon ?? getDefaultFilterIcon(config.type);

const isNonEmptyValue = (value: FilterValue): boolean => {
  if (value === null || value === undefined || value === "") {
    return false;
  }

  if (Array.isArray(value)) {
    return value.length > 0;
  }

  return true;
};

const toStringArray = (value: FilterValue): Array<string> => {
  if (value === null || value === undefined || value === "") {
    return [];
  }

  if (Array.isArray(value)) {
    return value.filter((item): item is string => typeof item === "string" && item !== "");
  }

  if (typeof value === "string") {
    return value === "" ? [] : [value];
  }

  if (typeof value === "number") {
    return [String(value)];
  }

  return [];
};

export const FilterBar = ({
  configs,
  initialValues = defaultInitialValues,
  maxVisibleFilters = 10,
  onFiltersChange,
}: FilterBarProps) => {
  const { t: translate } = useTranslation(["admin", "common"]);
  const [filters, setFilters] = useState<Array<FilterState>>(() =>
    Object.entries(initialValues)
      .filter(([, value]) => isNonEmptyValue(value))
      .map(([key, value]) => {
        const config = configs.find((con) => con.key === key);

        if (!config) {
          throw new Error(`Filter config not found for key: ${key}`);
        }

        return {
          config,
          id: `${key}-${Date.now()}`,
          value,
        };
      }),
  );

  const debouncedOnFiltersChange = useDebouncedCallback((filtersRecord: Record<string, FilterValue>) => {
    onFiltersChange(filtersRecord);
  }, 100);

  const updateFiltersRecord = useCallback(
    (updatedFilters: Array<FilterState>) => {
      const filtersRecord = updatedFilters.reduce<Record<string, FilterValue>>((accumulator, filter) => {
        if (!isNonEmptyValue(filter.value)) {
          return accumulator;
        }

        const { key } = filter.config;
        const existing = accumulator[key];

        if (existing === undefined) {
          accumulator[key] = filter.value;

          return accumulator;
        }

        const merged = [...new Set([...toStringArray(existing), ...toStringArray(filter.value)])];

        accumulator[key] = merged.length <= 1 ? (merged[0] ?? "") : merged;

        return accumulator;
      }, {});

      debouncedOnFiltersChange(filtersRecord);
    },
    [debouncedOnFiltersChange],
  );

  const addFilter = (config: FilterConfig) => {
    const newFilter: FilterState = {
      config,
      id: `${config.key}-${Date.now()}`,
      value: config.defaultValue ?? "",
    };

    const updatedFilters = [...filters, newFilter];

    setFilters(updatedFilters);
    updateFiltersRecord(updatedFilters);
  };

  const updateFilter = (id: string, value: FilterValue) => {
    const updatedFilters = filters.map((filter) => (filter.id === id ? { ...filter, value } : filter));

    setFilters(updatedFilters);
    updateFiltersRecord(updatedFilters);
  };

  const removeFilter = (id: string) => {
    const updatedFilters = filters.filter((filter) => filter.id !== id);

    setFilters(updatedFilters);
    updateFiltersRecord(updatedFilters);
  };

  const resetFilters = () => {
    setFilters([]);
    onFiltersChange({});
  };

  // If config.multiple is true, allow adding the same filter key again.
  // Otherwise keep original behavior (hide config once added).
  const availableConfigs = configs.filter((config) => {
    if (config.multiple === true) {
      return true;
    }

    return !filters.some((filter) => filter.config.key === config.key);
  });

  const renderFilter = (filter: FilterState) => {
    const props = {
      filter,
      onChange: (value: FilterValue) => updateFilter(filter.id, value),
      onRemove: () => removeFilter(filter.id),
    };

    switch (filter.config.type) {
      case "date":
        return <DateFilter key={filter.id} {...props} />;
      case "number":
        return <NumberFilter key={filter.id} {...props} />;
      case "select":
        return <SelectFilter key={filter.id} {...props} />;
      case "text":
        return <TextSearchFilter key={filter.id} {...props} />;
      default:
        return undefined;
    }
  };

  return (
    <HStack gap={2} wrap="wrap">
      {filters.slice(0, maxVisibleFilters).map(renderFilter)}
      {availableConfigs.length > 0 && (
        <Menu.Root>
          <Menu.Trigger asChild>
            <Button
              _hover={{ bg: "colorPalette.subtle" }}
              bg="gray.muted"
              borderRadius="full"
              variant="outline"
            >
              <MdAdd />
              {translate("common:filter")}
            </Button>
          </Menu.Trigger>
          <Menu.Content>
            {availableConfigs.map((config) => (
              <Menu.Item key={config.key} onClick={() => addFilter(config)} value={config.key}>
                <HStack gap={2}>
                  {getFilterIcon(config)}
                  {config.label}
                </HStack>
              </Menu.Item>
            ))}
          </Menu.Content>
        </Menu.Root>
      )}
      {filters.length > 0 && (
        <Button borderRadius="full" colorPalette="gray" onClick={resetFilters} size="sm" variant="outline">
          <MdClear />
          {translate("common:reset")}
        </Button>
      )}
    </HStack>
  );
};
