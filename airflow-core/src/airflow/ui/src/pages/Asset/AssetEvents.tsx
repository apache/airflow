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
import { Box } from "@chakra-ui/react";
import { useParams, useSearchParams } from "react-router-dom";

import { useAssetServiceGetAssetEvents } from "openapi/queries";
import { AssetEvents as AssetEventsTable } from "src/components/Assets/AssetEvents";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchParamsKeys } from "src/constants/searchParams";

export const AssetEvents = () => {
  const { assetId: assetIdParam } = useParams();
  const assetId = assetIdParam === undefined ? undefined : parseInt(assetIdParam, 10);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : ["-timestamp"];

  const { DAG_ID, END_DATE, START_DATE, TASK_ID } = SearchParamsKeys;
  const [searchParams] = useSearchParams();

  const { data, isLoading } = useAssetServiceGetAssetEvents(
    {
      assetId,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      sourceDagId: searchParams.get(DAG_ID) ?? undefined,
      sourceTaskId: searchParams.get(TASK_ID) ?? undefined,
      timestampGte: searchParams.get(START_DATE) ?? undefined,
      timestampLte: searchParams.get(END_DATE) ?? undefined,
    },
    undefined,
    { enabled: Boolean(assetId) },
  );

  const setOrderBy = (value: string) => {
    setTableURLState({
      pagination,
      sorting: [
        {
          desc: value.startsWith("-"),
          id: value.replace("-", ""),
        },
      ],
    });
  };

  return (
    <Box h="100%" overflow="auto" pt={2}>
      <AssetEventsTable
        assetId={assetId}
        data={data}
        isLoading={isLoading}
        setOrderBy={setOrderBy}
        setTableUrlState={setTableURLState}
        showFilters
        tableUrlState={tableURLState}
      />
    </Box>
  );
};
