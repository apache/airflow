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
import { Box, Heading, VStack } from "@chakra-ui/react";

import { UIAlertAccordion } from "src/components/UIAlertAccordion";

import { Health } from "./Health";
import { HistoricalMetrics } from "./HistoricalMetrics";
import { PoolSummary } from "./PoolSummary";
import { Stats } from "./Stats";

export const Dashboard = () => (
  <Box overflow="auto" px={4}>
    <VStack alignItems="start">
      <UIAlertAccordion />
      <Heading mb={2} size="2xl">
        Welcome
      </Heading>
    </VStack>
    <Box>
      <Stats />
    </Box>
    <Box display="flex" gap={8} mt={8}>
      <Health />
      <PoolSummary />
    </Box>
    <Box mt={8}>
      <HistoricalMetrics />
    </Box>
  </Box>
);
