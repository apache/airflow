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
import { Box, Heading, Text, VStack } from "@chakra-ui/react";

import type { UIAlert } from "openapi/requests/types.gen";
import { Accordion, Alert } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { Health } from "./Health";
import { HistoricalMetrics } from "./HistoricalMetrics";
import { Stats } from "./Stats";

export const Dashboard = () => {
  const confAlerts = useConfig("dashboard_alert");
  const alerts = Array.isArray(confAlerts) ? confAlerts : [];

  return (
    <Box>
      <VStack alignItems="start">
        {alerts.length > 0 ? (
          <Accordion.Root>
            {alerts.map((alert: UIAlert | null) =>
              alert ? (
                <Accordion.Item key={alert.text} value={alert.text}>
                  <Accordion.ItemTrigger />
                  <Accordion.ItemContent>
                    <Alert key={alert.text} status={alert.category ?? "info"}>
                      <Text fontSize="xl" padding="-0.5">
                        {alert.text}
                      </Text>
                    </Alert>
                  </Accordion.ItemContent>
                </Accordion.Item>
              ) : undefined,
            )}
          </Accordion.Root>
        ) : undefined}
        <Heading mb={4}>Welcome</Heading>
      </VStack>
      <Box>
        <Health />
      </Box>
      <Box mt={5}>
        <Stats />
      </Box>
      <Box mt={5}>
        <HistoricalMetrics />
      </Box>
    </Box>
  );
};
