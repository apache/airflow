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
  Heading,
  HStack,
  VStack,
  StackSeparator,
  Skeleton,
} from "@chakra-ui/react";

import { useAssetServiceGetAssetEvents } from "openapi/queries";
import { MetricsBadge } from "src/components/MetricsBadge";

import { AssetEvent } from "./AssetEvent";

type AssetEventProps = {
  readonly endDate: string;
  readonly startDate: string;
};

export const AssetEvents = ({ endDate, startDate }: AssetEventProps) => {
  const { data, isLoading } = useAssetServiceGetAssetEvents({
    limit: 5,
    orderBy: "-timestamp", // Make it sortable from UI
    timestampGte: startDate,
    timestampLte: endDate,
  });

  return (
    <Box borderWidth={1} ml={2} pb={2}>
      <HStack mt={0} p={3}>
        <MetricsBadge
          backgroundColor="blue.solid"
          runs={isLoading ? 0 : data?.total_entries}
        />
        <Heading size="md">Asset Events</Heading>
      </HStack>
      {isLoading ? (
        <VStack px={3} separator={<StackSeparator />}>
          {Array.from({ length: 5 }, (_, index) => index).map((index) => (
            <Skeleton height={100} key={index} width="full" />
          ))}
        </VStack>
      ) : (
        <VStack px={3} separator={<StackSeparator />}>
          {data?.asset_events.map((event) => (
            <AssetEvent event={event} key={event.id} />
          ))}
        </VStack>
      )}
    </Box>
  );
};
