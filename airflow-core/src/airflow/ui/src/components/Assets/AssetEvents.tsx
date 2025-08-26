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
import { Box, Heading, Flex, HStack, Skeleton, Separator, Text, VStack } from "@chakra-ui/react";
import type { BoxProps } from "@chakra-ui/react";
import { createListCollection } from "@chakra-ui/react/collection";
import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { FiDatabase } from "react-icons/fi";

import type { AssetEventCollectionResponse, AssetEventResponse } from "openapi/requests/types.gen";
import { DateTimeInput } from "src/components/DateTimeInput";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useFilterSearchParams } from "src/utils/useFilterSearchParams";

import { DataTable } from "../DataTable";
import type { CardDef, TableState } from "../DataTable/types";
import { SearchBar } from "../SearchBar";
import { AssetEvent } from "./AssetEvent";

const cardDef = (assetId?: number): CardDef<AssetEventResponse> => ({
  card: ({ row }) => <AssetEvent assetId={assetId} event={row} />,
  meta: {
    customSkeleton: <Skeleton height="120px" width="100%" />,
  },
});

type AssetEventProps = {
  readonly assetId?: number;
  readonly data?: AssetEventCollectionResponse;
  readonly isLoading?: boolean;
  readonly setOrderBy?: (order: string) => void;
  readonly setTableUrlState?: (state: TableState) => void;
  readonly tableUrlState?: TableState;
  readonly titleKey?: string;
};

export const AssetEvents = ({
  assetId,
  data,
  isLoading,
  setOrderBy,
  setTableUrlState,
  tableUrlState,
  titleKey,
  ...rest
}: AssetEventProps & BoxProps) => {
  const { t: translate } = useTranslation(["dashboard", "common", "dag"]);
  const assetSortOptions = createListCollection({
    items: [
      { label: translate("sortBy.newestFirst"), value: "-timestamp" },
      { label: translate("sortBy.oldestFirst"), value: "timestamp" },
    ],
  });
  const runTypes = ["", "asset_triggered", "backfill", "manual", "scheduled"];
  const runTypeOptions = createListCollection({
    items: runTypes.map((type) => ({
      label: type === "" ? translate(`common:expression.all`) : translate(`common:runTypes.${type}`),
      value: type,
    })),
  });
  const { DAG_ID_PATTERN, END_DATE, RUN_TYPE, START_DATE, TASK_ID_PATTERN } = SearchParamsKeys;
  const { dagIdPattern, endDate, handleFilterChange, runType, startDate, taskIdPattern } =
    useFilterSearchParams();
  const filteredData = useMemo(() => {
    if (!data) {
      return {
        asset_events: [],
        total_entries: 0,
      };
    }
    const filteredAssetEvents = data.asset_events.filter((event) => {
      if (startDate !== "" && new Date(startDate) > new Date(event.timestamp)) {
        return false;
      }
      if (endDate !== "" && new Date(endDate) < new Date(event.timestamp)) {
        return false;
      }
      if (dagIdPattern !== "" && !event.source_dag_id?.includes(dagIdPattern)) {
        return false;
      }
      if (taskIdPattern !== "" && !event.source_task_id?.includes(taskIdPattern)) {
        return false;
      }
      if (runType !== "" && !event.source_run_id?.startsWith(`${runType}__`)) {
        return false;
      }

      return true;
    });

    return {
      asset_events: filteredAssetEvents,
      total_entries: filteredAssetEvents.length,
    };
  }, [data, startDate, endDate, dagIdPattern, taskIdPattern, runType]);

  return (
    <Box borderBottomWidth={0} borderRadius={5} borderWidth={1} p={4} py={2} {...rest}>
      <Flex alignItems="center" justify="space-between">
        <HStack>
          <StateBadge colorPalette="blue" fontSize="md" variant="solid">
            <FiDatabase />
            {data?.total_entries ?? " "}
          </StateBadge>
          <Heading marginEnd="auto" size="md">
            {translate(titleKey ?? "common:assetEvent", { count: data?.total_entries ?? 0 })}
          </Heading>
        </HStack>
        {setOrderBy === undefined ? undefined : (
          <Select.Root
            borderWidth={0}
            collection={assetSortOptions}
            data-testid="asset-sort-duration"
            defaultValue={["-timestamp"]}
            onValueChange={(option) => setOrderBy(option.value[0] as string)}
            size="sm"
            width={130}
          >
            <Select.Trigger>
              <Select.ValueText placeholder="Sort by" />
            </Select.Trigger>

            <Select.Content>
              {assetSortOptions.items.map((option) => (
                <Select.Item item={option} key={option.value[0]}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        )}
      </Flex>
      <VStack align="start" gap={4} paddingY="4px">
        <HStack flexWrap="wrap" gap={4}>
          <Box w="200px">
            <Text fontSize="xs">{translate("common:table.from")}</Text>
            <DateTimeInput
              onChange={(event) => handleFilterChange(START_DATE)(event.target.value)}
              value={startDate}
            />
          </Box>
          <Box w="200px">
            <Text fontSize="xs">{translate("common:table.to")}</Text>
            <DateTimeInput
              onChange={(event) => handleFilterChange(END_DATE)(event.target.value)}
              value={endDate}
            />
          </Box>
          <Box w="200px">
            <Text fontSize="xs">{translate("common:filters.dagDisplayNamePlaceholder")}</Text>
            <SearchBar
              defaultValue={dagIdPattern}
              hideAdvanced
              hotkeyDisabled={true}
              onChange={handleFilterChange(DAG_ID_PATTERN)}
              placeHolder={translate("common:filters.dagDisplayNamePlaceholder")}
            />
          </Box>
          <Box w="200px">
            <Text fontSize="xs">{translate("common:filters.taskIdPlaceholder")}</Text>
            <SearchBar
              defaultValue={taskIdPattern}
              hideAdvanced
              hotkeyDisabled={true}
              onChange={handleFilterChange(TASK_ID_PATTERN)}
              placeHolder={translate("common:filters.taskIdPlaceholder")}
            />
          </Box>
          <Box w="200px">
            <Text fontSize="xs">{translate("common:dagRun.runType")}</Text>
            <Select.Root
              borderWidth={0}
              collection={runTypeOptions}
              defaultValue={[runType]}
              onValueChange={(option) => handleFilterChange(RUN_TYPE)(option.value[0] as string)}
            >
              <Select.Trigger>
                <Select.ValueText />
              </Select.Trigger>

              <Select.Content>
                {runTypeOptions.items.map((option) => (
                  <Select.Item item={option} key={option.value[0]}>
                    {option.label}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
          </Box>
        </HStack>
      </VStack>
      <Separator mt={2.5} />
      <DataTable
        cardDef={cardDef(assetId)}
        columns={[]}
        data={filteredData.asset_events}
        displayMode="card"
        initialState={tableUrlState}
        isLoading={isLoading}
        modelName={translate("common:assetEvent_one")}
        noRowsMessage={translate("noAssetEvents")}
        onStateChange={setTableUrlState}
        skeletonCount={5}
        total={filteredData.total_entries}
      />
    </Box>
  );
};
