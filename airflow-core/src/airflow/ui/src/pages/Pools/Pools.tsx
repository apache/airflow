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
import { Box, HStack, Skeleton, Spacer } from "@chakra-ui/react";
import { createListCollection } from "@chakra-ui/react/collection";
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { usePoolServiceGetPools } from "openapi/queries";
import { PoolResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { type SearchParamsKeysType, SearchParamsKeys } from "src/constants/searchParams";

import { DataTable } from "src/components/DataTable";
import { CardDef } from "src/components/DataTable/types";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";

import AddPoolButton from "./AddPoolButton";
import PoolBar from "./PoolBar";
import { Select } from "src/components/ui";

const cardDef = (): CardDef<PoolResponse> => ({
  card: ({ row }) => <PoolBar key={row.name} pool={row} />,
  meta: {
    customSkeleton: <Skeleton height="100px" width="100%" />,
  },
});

const poolSortOptions = createListCollection({
  items: [
    { label: "Name (A-Z)", value: "name"},
    { label: "Name (Z-A)", value: "-name"}
  ]
});

export const Pools = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;
  const [poolNamePattern, setPoolNamePattern] = useState(searchParams.get(NAME_PATTERN_PARAM) ?? undefined);

  const { setTableURLState,tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? `${sort.desc ? "-" : ""}${sort.id}` : "name";

  const { data, error, isLoading } = usePoolServiceGetPools({
    poolNamePattern: poolNamePattern ?? undefined,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
  });

  const handleSearchChange = (value: string) => {
    if (value) {
      searchParams.set(NAME_PATTERN_PARAM, value);
    } else {
      searchParams.delete(NAME_PATTERN_PARAM);
    }
    setSearchParams(searchParams);
    setPoolNamePattern(value);
  };

  const handleSortChange = (value: string, setTableURLState: any) => { 
    setTableURLState((prevState) => ({
      ...prevState,
      sorting: [{ id: value.replace("-", ""), desc: value.startsWith("-") }],
    }));
  };
  
  return (
    <>
      <ErrorAlert error={error} />
      <SearchBar
        buttonProps={{ disabled: true }}
        defaultValue={poolNamePattern ?? ""}
        onChange={handleSearchChange}
        placeHolder="Search Pools"
      />
      <HStack gap={4} mt={4}>
        <Select.Root
          borderWidth={0}
          collection={poolSortOptions}
          defaultValue={["name"]}
          onValueChange={(value) => handleSortChange(value, setTableURLState)}
          width={130}
        >
          <Select.Trigger>
            <Select.ValueText placeHolder="Sort by" />
          </Select.Trigger>
          
          <Select.Content>
            {poolSortOptions.items.map((option) => (
              <Select.Item item={option} key={option.value[0]}>
                {option.label}  
              </Select.Item>
            ))}
          </Select.Content>
        </Select.Root>
        <AddPoolButton />
      </HStack>
      <Box mt={4}>
        <DataTable
          cardDef={cardDef()}
          displayMode="card"
          onStateChange={setTableURLState}
          initialState={tableURLState}  
          isLoading={isLoading}
          columns={[]}
          modelName="Pools"
          total={data ? data.total_entries : 0} 
          data={data ? data.pools :[]}        />  
      </Box>
    </>
  );
};
