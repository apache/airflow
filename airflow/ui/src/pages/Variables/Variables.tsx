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
import {
  Box,
  Button,
  createListCollection,
  HStack,
  Input,
} from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import { useState, useEffect } from "react";
import { FiUpload, FiFilter } from "react-icons/fi";
import { IoAddCircleOutline } from "react-icons/io5";

import { useVariableServiceGetVariables } from "openapi/queries";
import type { VariableResponse } from "openapi/requests/types.gen";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { CloseButton, Select } from "src/components/ui";

const variablesColumn = (): Array<ColumnDef<VariableResponse>> => [
  {
    accessorKey: "key",
    enableSorting: true,
    header: "Key",
    meta: {
      skeletonWidth: 20,
    },
  },
  {
    accessorKey: "value",
    enableSorting: true,
    header: "Value",
    meta: {
      skeletonWidth: 20,
    },
  },
  {
    accessorKey: "description",
    enableSorting: false,
    header: "Description",
    meta: {
      skeletonWidth: 60,
    },
  },
];

const filterOptions = createListCollection({
  items: [
    { label: "Key", value: "key" },
    { label: "Value", value: "value" },
  ],
});

const filterOperatorOptions = createListCollection({
  items: [
    { label: "Starts With", value: "starts_with" },
    { label: "Ends With", value: "ends_with" },
    { label: "Contains", value: "contains" },
    { label: "Equals", value: "equals" },
    { label: "Not Starts With", value: "not_starts_with" },
    { label: "Not Ends With", value: "not_ends_with" },
  ],
});

export const Variables = () => {
  const { setTableURLState, tableURLState } = useTableURLState({
    sorting: [{ desc: true, id: "key" }],
  });
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;

  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : undefined;

  const [filterType, setFilterType] = useState<string | undefined>(undefined);
  const [filterOperator, setFilterOperator] = useState<string | undefined>(
    undefined,
  );
  const [filterText, setFilterText] = useState<string>("");

  const {
    data,
    error: VariableError,
    isFetching,
    isLoading,
  } = useVariableServiceGetVariables({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const [filteredData, setFilteredData] = useState<Array<VariableResponse>>([]);

  useEffect(() => {
    if (data?.variables) {
      let filtered = data.variables;

      if (filterText.trim()) {
        filtered = filtered.filter((item) => {
          const valueToFilter =
            filterType === "key" ? item.key : (item.value ?? "");

          switch (filterOperator) {
            case "contains":
              return valueToFilter.includes(filterText);
            case "ends_with":
              return valueToFilter.endsWith(filterText);
            case "equals":
              return valueToFilter === filterText;
            case "not_ends_with":
              return !valueToFilter.endsWith(filterText);
            case "not_starts_with":
              return !valueToFilter.startsWith(filterText);
            case "starts_with":
              return valueToFilter.startsWith(filterText);
            default:
              return true;
          }
        });
      }

      setFilteredData(filtered);
    } else {
      setFilteredData([]);
    }

    // Reset pagination when filter changes
    setTableURLState({
      pagination: { ...pagination, pageIndex: 0 },
      sorting,
    });
  }, [
    filterText,
    filterType,
    filterOperator,
    data,
    pagination,
    sorting,
    setTableURLState,
  ]);

  return (
    <Box>
      <HStack justifyContent="space-between">
        <Box
          alignItems="center"
          border="1px solid"
          borderColor="gray.800"
          borderRadius="md"
          color="gray.400"
          display="flex"
          gap={2}
          p={1}
          pl={2}
          position="relative"
        >
          <FiFilter />
          <Select.Root
            collection={filterOptions}
            onValueChange={(event) => setFilterType(event.value[0])}
            size="xs"
            value={[filterType ?? ""]}
            width="90px"
          >
            <Select.Trigger>
              <Select.ValueText placeholder="Filter by" />
            </Select.Trigger>
            <Select.Content>
              {filterOptions.items.map((option) => (
                <Select.Item item={option} key={option.value}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <Select.Root
            collection={filterOperatorOptions}
            onValueChange={(event) => setFilterOperator(event.value[0])}
            size="xs"
            value={[filterOperator ?? ""]}
            width="140px"
          >
            <Select.Trigger>
              <Select.ValueText placeholder="Filter operator" />
            </Select.Trigger>
            <Select.Content>
              {filterOperatorOptions.items.map((option) => (
                <Select.Item item={option} key={option.value}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
          <Input
            border="0px"
            onChange={(event) => setFilterText(event.target.value)}
            outline="none"
            placeholder="Filter text"
            size="xs"
            value={filterText}
            width="200px"
          />
          {filterText === "" ? undefined : (
            <CloseButton
              aria-label="Clear search"
              colorPalette="gray"
              data-testid="clear-search"
              onClick={() => {
                setFilterText("");
              }}
              size="xs"
            />
          )}
        </Box>

        <HStack>
          <Button colorPalette="blue">
            <FiUpload />
            Import Variables
          </Button>
          <Button colorPalette="blue">
            <IoAddCircleOutline />
            Add Variable
          </Button>
        </HStack>
      </HStack>

      <ErrorAlert error={VariableError} />
      <DataTable
        columns={variablesColumn()}
        data={filteredData}
        displayMode="table"
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="Variable"
        onStateChange={setTableURLState}
        skeletonCount={undefined}
        total={filteredData.length}
      />
    </Box>
  );
};
