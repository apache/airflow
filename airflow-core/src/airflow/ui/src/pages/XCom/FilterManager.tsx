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
import { MdAdd } from "react-icons/md";

import { Menu } from "src/components/ui";

import { FilterPill, type FilterType } from "./FilterPill";

type FilterState = {
  id: string;
  type: FilterType;
  value: string;
};

type FilterManagerProps = {
  readonly initialFilters?: Record<FilterType, string>;
  readonly onFiltersChange: (filters: Record<FilterType, string>) => void;
};

const FILTER_LABELS: Record<FilterType, string> = {
  dag_id: "DAG ID",
  key: "Key",
  run_id: "Run ID",
  task_id: "Task ID",
};

const defaultInitialFilters: Record<FilterType, string> = {
  dag_id: "",
  key: "",
  run_id: "",
  task_id: "",
};

export const FilterManager = ({
  initialFilters = defaultInitialFilters,
  onFiltersChange,
}: FilterManagerProps) => {
  const { t: translate } = useTranslation();
  const [filters, setFilters] = useState<Array<FilterState>>(() =>
    Object.entries(initialFilters)
      .filter(([, value]) => value !== "")
      .map(([type, value]) => ({
        id: `${type}-${Date.now()}`,
        type: type as FilterType,
        value,
      })),
  );

  const addFilter = useCallback((filterType: FilterType) => {
    const newFilter: FilterState = {
      id: `${filterType}-${Date.now()}`,
      type: filterType,
      value: "",
    };

    setFilters((previous) => [...previous, newFilter]);
  }, []);

  const updateFilter = useCallback(
    (id: string, value: string) => {
      setFilters((previous) => previous.map((filter) => (filter.id === id ? { ...filter, value } : filter)));

      const updatedFilters = filters.reduce(
        (accumulator, filter) => {
          accumulator[filter.type] = filter.id === id ? value : filter.value;

          return accumulator;
        },
        {} as Record<FilterType, string>,
      );

      onFiltersChange(updatedFilters);
    },
    [filters, onFiltersChange],
  );

  const removeFilter = useCallback(
    (id: string) => {
      const filterToRemove = filters.find((filter) => filter.id === id);

      setFilters((previous) => previous.filter((filter) => filter.id !== id));

      if (filterToRemove) {
        const updatedFilters = filters
          .filter((filter) => filter.id !== id)
          .reduce(
            (accumulator, filter) => {
              accumulator[filter.type] = filter.value;

              return accumulator;
            },
            {} as Record<FilterType, string>,
          );

        onFiltersChange(updatedFilters);
      }
    },
    [filters, onFiltersChange],
  );

  const availableFilterTypes = Object.keys(FILTER_LABELS).filter(
    (type) => !filters.some((filter) => filter.type === type),
  ) as Array<FilterType>;

  return (
    <HStack gap={2} wrap="wrap">
      {filters.map((filter) => (
        <FilterPill
          filterType={filter.type}
          key={filter.id}
          label={FILTER_LABELS[filter.type]}
          onRemove={() => removeFilter(filter.id)}
          onValueChange={(value) => updateFilter(filter.id, value)}
          value={filter.value}
        />
      ))}
      {availableFilterTypes.length > 0 && (
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
            {availableFilterTypes.map((filterType) => (
              <Menu.Item key={filterType} onClick={() => addFilter(filterType)} value={filterType}>
                {FILTER_LABELS[filterType]}
              </Menu.Item>
            ))}
          </Menu.Content>
        </Menu.Root>
      )}
    </HStack>
  );
};
