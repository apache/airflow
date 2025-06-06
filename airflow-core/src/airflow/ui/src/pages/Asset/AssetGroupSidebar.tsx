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
import { Box, Heading, Text, HStack, VStack, Badge, Button } from "@chakra-ui/react";

import type { AssetResponse } from "openapi/requests/types.gen";

type AssetGroupSidebarProps = {
  assets: Array<AssetResponse>;
  groupName: string;
};

export const AssetGroupSidebar: React.FC<AssetGroupSidebarProps> = ({ assets, groupName }) => {
  const producingTasks = assets.reduce((acc, asset) => acc + asset.producing_tasks.length, 0);
  const consumingDags = assets.reduce((acc, asset) => acc + asset.consuming_dags.length, 0);

  // Collect all last asset events from all assets
  const events = assets
    .map((asset) => asset.last_asset_event)
    .filter((ev): ev is NonNullable<typeof ev> => Boolean(ev));

  return (
    <VStack align="stretch" gap={4}>
      <Box bg="chakra-subtle-bg" borderColor="chakra-border-color" borderRadius="md" borderWidth={1} p={4}>
        <HStack gap={2} mb={2}>
          <Badge aria-label="database icon" colorScheme="gray" fontSize="lg">
            🗄️
          </Badge>
          <Heading size="md">{groupName}</Heading>
        </HStack>
        <Text color="chakra-fg" fontSize="sm" mb={2}>
          <b>{/* i18n: Group label */}Group</b>
          <br />
          {groupName}
        </Text>
        <HStack gap={4} mb={2}>
          <Box>
            <Text color="chakra-fg" fontSize="xs">
              {/* i18n: Producing Tasks label */}Producing Tasks
            </Text>
            <Button disabled size="sm" variant="outline">
              {producingTasks} {/* i18n: Task label */}Task{producingTasks === 1 ? "" : "s"}
            </Button>
          </Box>
          <Box>
            <Text color="chakra-fg" fontSize="xs">
              {/* i18n: Consuming DAGs label */}Consuming DAGs
            </Text>
            <Button disabled size="sm" variant="outline">
              {consumingDags} {/* i18n: DAG label */}DAG{consumingDags === 1 ? "" : "s"}
            </Button>
          </Box>
        </HStack>
      </Box>
      <Box bg="chakra-subtle-bg" borderColor="chakra-border-color" borderRadius="md" borderWidth={1} p={4}>
        <HStack mb={2}>
          <Badge colorScheme="blue" />
          <Heading size="sm">{/* i18n: Asset Events label */}Asset Events</Heading>
        </HStack>
        {events.length === 0 ? (
          <Text color="chakra-fg-subtle" fontSize="sm">
            {/* i18n: No events label */}No events
          </Text>
        ) : (
          events.map((ev) => (
            <Box bg="chakra-subtle-bg-hover" borderRadius="md" key={ev.id} mb={2} p={2}>
              <Text color="chakra-fg-subtle" fontSize="xs">
                {ev.timestamp}
              </Text>
              <Text color="chakra-fg" fontSize="sm">
                {ev.id ?? ""}
              </Text>
            </Box>
          ))
        )}
      </Box>
    </VStack>
  );
};
