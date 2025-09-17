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

export const FilterBar = ({
  configs,
  initialValues = defaultInitialValues,
  maxVisibleFilters = 10,
  onFiltersChange,
}: FilterBarProps) => {
  const { t: translate } = useTranslation(["admin", "common"]);

  const [filters, setFilters] = useState<Array<FilterState>>(() =>
    Object.entries(initialValues)
      .filter(([, value]) => value !== null && value !== undefined && value !== "")
      .map(([key, value]) => {
        const configForKey = configs.find((configItem) => configItem.key === key);

        if (!configForKey) {
          throw new Error(`Filter config not found for key: ${key}`);
        }

        return {
          config: configForKey,
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
      const filtersRecord = updatedFilters.reduce<Record<string, FilterValue>>((accumulator, filterState) => {
        if (filterState.value !== null && filterState.value !== undefined && filterState.value !== "") {
          accumulator[filterState.config.key] = filterState.value;
        }

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
    const updated = [...filters, newFilter];

    setFilters(updated);
    updateFiltersRecord(updated);
  };

  const updateFilter = (id: string, value: FilterValue) => {
    const updated = filters.map((filterState) =>
      filterState.id === id ? { ...filterState, value } : filterState,
    );

    setFilters(updated);
    updateFiltersRecord(updated);
  };

  const removeFilter = (id: string) => {
    const updated = filters.filter((filterState) => filterState.id !== id);

    setFilters(updated);
    updateFiltersRecord(updated);
  };

  const resetFilters = () => {
    setFilters([]);
    onFiltersChange({});
  };

  const availableConfigs = configs.filter(
    (configItem) => !filters.some((filterState) => filterState.config.key === configItem.key),
  );

  const renderFilter = (filter: FilterState) => {
    const commonProps = {
      filter,
      onChange: (value: FilterValue) => updateFilter(filter.id, value),
      onRemove: () => removeFilter(filter.id),
    };

    switch (filter.config.type) {
      case "date":
        return <DateFilter key={filter.id} {...commonProps} />;
      case "number":
        return <NumberFilter key={filter.id} {...commonProps} />;
      case "select":
        return <SelectFilter key={filter.id} {...commonProps} />;
      case "text":
        return <TextSearchFilter key={filter.id} {...commonProps} />;
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
            {availableConfigs.map((configItem) => (
              <Menu.Item key={configItem.key} onClick={() => addFilter(configItem)} value={configItem.key}>
                <HStack gap={2}>
                  {getFilterIcon(configItem)}
                  {configItem.label}
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
