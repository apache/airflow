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
import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { useAssetServiceGetAssetEvents, useTaskInstanceServiceGetMappedTaskInstance } from "openapi/queries";
import { AssetEvents as AssetEventsTable } from "src/components/Assets/AssetEvents";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { SearchBar } from "src/components/SearchBar";
import { SearchParamsKeys } from "src/constants/searchParams";
import { isStatePending, useAutoRefresh } from "src/utils";

export const AssetEvents = () => {
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const { t: translate } = useTranslation(["assets"]);

  const [assetNameSearch, setAssetNameSearch] = useState(
    searchParams.get(SearchParamsKeys.NAME_PATTERN) ?? "",
  );

  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    },
    undefined,
    {
      enabled: !isNaN(parsedMapIndex),
    },
  );

  const refetchInterval = useAutoRefresh({ dagId });

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;

  const handleSearchChange = useCallback(
    (value: string) => {
      setAssetNameSearch(value);
      // Reset pagination when searching
      setTableURLState({
        ...tableURLState,
        pagination: { ...pagination, pageIndex: 0 },
      });
      if (value) {
        searchParams.set(SearchParamsKeys.NAME_PATTERN, value);
      } else {
        searchParams.delete(SearchParamsKeys.NAME_PATTERN);
      }
      setSearchParams(searchParams);
    },
    [pagination, searchParams, setSearchParams, setTableURLState, tableURLState],
  );

  const handleOrderByChange = useCallback(
    (order: string) => {
      setTableURLState({
        ...tableURLState,
        sorting: [{ desc: order.startsWith("-"), id: order.startsWith("-") ? order.slice(1) : order }],
      });
    },
    [setTableURLState, tableURLState],
  );

  const orderBy =
    sorting.length > 0 && sorting[0] ? [`${sorting[0].desc ? "-" : ""}${sorting[0].id}`] : ["-timestamp"];

  const { data: assetEventsData, isLoading } = useAssetServiceGetAssetEvents(
    {
      limit: pagination.pageSize,
      namePattern: assetNameSearch || undefined,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
      sourceDagId: dagId,
      sourceMapIndex: parseInt(mapIndex, 10),
      sourceRunId: runId,
      sourceTaskId: taskId,
    },
    undefined,
    {
      refetchInterval: () => (isStatePending(taskInstance?.state) ? refetchInterval : false),
    },
  );

  return (
    <Box>
      <Box maxWidth="500px" mb={4}>
        <SearchBar
          defaultValue={assetNameSearch}
          hotkeyDisabled
          onChange={handleSearchChange}
          placeholder={translate("searchPlaceholder")}
        />
      </Box>
      <AssetEventsTable
        data={assetEventsData}
        isLoading={isLoading}
        setOrderBy={handleOrderByChange}
        setTableUrlState={setTableURLState}
        tableUrlState={tableURLState}
        titleKey="common:createdAssetEvent"
      />
    </Box>
  );
};
