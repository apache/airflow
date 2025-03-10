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
import { useState } from "react";
import { useSearchParams } from "react-router-dom";

import { usePoolServiceGetPools } from "openapi/queries";
import { ErrorAlert } from "src/components/ErrorAlert";
import { SearchBar } from "src/components/SearchBar";
import { type SearchParamsKeysType, SearchParamsKeys } from "src/constants/searchParams";

import AddPoolButton from "./AddPoolButton";
import PoolBar from "./PoolBar";

export const Pools = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { NAME_PATTERN: NAME_PATTERN_PARAM }: SearchParamsKeysType = SearchParamsKeys;
  const [poolNamePattern, setPoolNamePattern] = useState(searchParams.get(NAME_PATTERN_PARAM) ?? undefined);
  const { data, error, isLoading } = usePoolServiceGetPools({
    poolNamePattern: poolNamePattern ?? undefined,
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
        <Spacer />
        <AddPoolButton />
      </HStack>
      <Box mt={4}>
        {isLoading ? (
          <Skeleton height="100px" />
        ) : (
          data?.pools.map((pool) => <PoolBar key={pool.name} pool={pool} />)
        )}
      </Box>
    </>
  );
};
