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
import { Box, Heading, Flex, HStack, Skeleton } from "@chakra-ui/react";
import { createListCollection } from "@chakra-ui/react/collection";
import { FiDatabase } from "react-icons/fi";

import type { AssetEventCollectionResponse, AssetEventResponse } from "openapi/requests/types.gen";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";
import { pluralize } from "src/utils";

import { DataTable } from "../DataTable";
import type { CardDef, TableState } from "../DataTable/types";
import { AssetEvent } from "./AssetEvent";

const cardDef = (assetId?: number, showExtra?: boolean): CardDef<AssetEventResponse> => ({
  card: ({ row }) => <AssetEvent assetId={assetId} event={row} showExtra={showExtra} />,
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
  readonly showExtra?: boolean;
  readonly tableUrlState?: TableState;
  readonly title?: string;
};

export const AssetEvents = ({
  assetId,
  data,
  isLoading,
  setOrderBy,
  setTableUrlState,
  showExtra,
  tableUrlState,
  title,
}: AssetEventProps) => {
  const assetSortOptions = createListCollection({
    items: [
      { label: "Newest first", value: "-timestamp" },
      { label: "Oldest first", value: "timestamp" },
    ],
  });

  return (
    <Box borderBottomWidth={0} borderRadius={5} borderWidth={1} ml={2}>
      <Flex justify="space-between" mr={1} mt={0} pl={3} pt={1}>
        <HStack>
          <StateBadge colorPalette="blue" fontSize="md" variant="solid">
            <FiDatabase />
            {data?.total_entries ?? " "}
          </StateBadge>
          <Heading marginEnd="auto" size="md">
            {pluralize(title ?? "Asset Event", data?.total_entries ?? 0, undefined, true)}
          </Heading>
        </HStack>
        {setOrderBy === undefined ? undefined : (
          <Select.Root
            borderWidth={0}
            collection={assetSortOptions}
            data-testid="asset-sort-duration"
            defaultValue={["-timestamp"]}
            onValueChange={(option) => setOrderBy(option.value[0] as string)}
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
      <DataTable
        cardDef={cardDef(assetId, showExtra)}
        columns={[]}
        data={data?.asset_events ?? []}
        displayMode="card"
        initialState={tableUrlState}
        isLoading={isLoading}
        modelName="Asset Event"
        onStateChange={setTableUrlState}
        skeletonCount={5}
        total={data?.total_entries}
      />
    </Box>
  );
};
