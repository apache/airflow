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

import type { UIAlert } from "openapi/requests/types.gen";
import ReactMarkdown from "src/components/ReactMarkdown";
import { Accordion, Alert } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { Health } from "./Health";
import { HistoricalMetrics } from "./HistoricalMetrics";
import { Stats } from "./Stats";

export const Dashboard = () => {
  const alerts = useConfig("dashboard_alert") as Array<UIAlert>;

  return (
    <Box overflow="auto" px={4}>
      <VStack alignItems="start">
        {alerts.length > 0 ? (
          <Accordion.Root collapsible defaultValue={["ui_alerts"]}>
            <Accordion.Item key="ui_alerts" value="ui_alerts">
              {alerts.map((alert: UIAlert, index) =>
                index === 0 ? (
                  <Accordion.ItemTrigger key={alert.text} mb={2}>
                    <Alert status={alert.category}>
                      <ReactMarkdown>{alert.text}</ReactMarkdown>
                    </Alert>
                  </Accordion.ItemTrigger>
                ) : (
                  <Accordion.ItemContent key={alert.text} pr={8}>
                    <Alert status={alert.category}>
                      <ReactMarkdown>{alert.text}</ReactMarkdown>
                    </Alert>
                  </Accordion.ItemContent>
                ),
              )}
            </Accordion.Item>
          </Accordion.Root>
        ) : undefined}
        <Heading mb={2} size="2xl">
          Welcome
        </Heading>
      </VStack>
      <Box>
        <Stats />
      </Box>
      <Box mt={8}>
        <Health />
      </Box>
      <Box mt={8}>
        <HistoricalMetrics />
      </Box>
    </Box>
  );
};
