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
import { Box, Heading, Flex, HStack, VStack, StackSeparator, Skeleton } from "@chakra-ui/react";
import { createListCollection } from "@chakra-ui/react/collection";
import { FiDatabase } from "react-icons/fi";

import { useAssetServiceGetAssetEvents } from "openapi/queries";
import { StateBadge } from "src/components/StateBadge";
import { Select } from "src/components/ui";

import { AssetEvent } from "./AssetEvent";

type AssetEventProps = {
  readonly assetSortBy: string;
  readonly endDate: string;
  readonly setAssetSortBy: React.Dispatch<React.SetStateAction<string>>;
  readonly startDate: string;
};

export const AssetEvents = ({ assetSortBy, endDate, setAssetSortBy, startDate }: AssetEventProps) => {
  const { data, isLoading } = useAssetServiceGetAssetEvents({
    limit: 6,
    orderBy: assetSortBy,
    timestampGte: startDate,
    timestampLte: endDate,
  });

  const assetSortOptions = createListCollection({
    items: [
      { label: "Newest first", value: "-timestamp" },
      { label: "Oldest first", value: "timestamp" },
    ],
  });

  return (
    <Box borderRadius={5} borderWidth={1} ml={2} pb={2}>
      <Flex justify="space-between" mr={1} mt={0} pl={3} pt={1}>
        <HStack>
          <StateBadge colorPalette="blue" fontSize="md" variant="solid">
            <FiDatabase />
            {data?.total_entries ?? " "}
          </StateBadge>
          <Heading marginEnd="auto" size="md">
            Asset Events
          </Heading>
        </HStack>
        <Select.Root
          borderWidth={0}
          collection={assetSortOptions}
          data-testid="asset-sort-duration"
          defaultValue={["-timestamp"]}
          onValueChange={(option) => setAssetSortBy(option.value[0] as string)}
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
      </Flex>
      {isLoading ? (
        <VStack px={3} separator={<StackSeparator />}>
          {Array.from({ length: 5 }, (_, index) => index).map((index) => (
            <Skeleton height={100} key={index} width="full" />
          ))}
        </VStack>
      ) : (
        <VStack px={3} separator={<StackSeparator />}>
          {data?.asset_events.map((event) => <AssetEvent event={event} key={event.id} />)}
        </VStack>
      )}
    </Box>
  );
};
