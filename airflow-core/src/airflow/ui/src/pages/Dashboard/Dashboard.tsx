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
import dayjs from "dayjs";
import { useState } from "react";
import { useTranslation } from "react-i18next";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { ReactAppResponse, UIAlert } from "openapi/requests/types.gen";
import ReactMarkdown from "src/components/ReactMarkdown";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { Accordion, Alert } from "src/components/ui";
import { useConfig } from "src/queries/useConfig";

import { ReactPlugin } from "../ReactPlugin";
import { DashboardDeadlines } from "./Deadlines";
import { FavoriteDags } from "./FavoriteDags";
import { Health } from "./Health";
import { HistoricalMetrics } from "./HistoricalMetrics";
import { PoolSummary } from "./PoolSummary";
import { Stats } from "./Stats";

const defaultHour = "24";

export const Dashboard = () => {
  const alerts = useConfig("dashboard_alert") as Array<UIAlert>;
  const { t: translate } = useTranslation("dashboard");
  const instanceName = useConfig("instance_name");

  const now = dayjs();
  const [startDate, setStartDate] = useState(now.subtract(Number(defaultHour), "hour").toISOString());
  const [endDate, setEndDate] = useState(now.toISOString());

  const { data: pluginData } = usePluginServiceGetPlugins();

  const dashboardReactPlugins =
    pluginData?.plugins
      .flatMap((plugin) => plugin.react_apps)
      .filter((reactAppPlugin: ReactAppResponse) => reactAppPlugin.destination === "dashboard") ?? [];

  return (
    <Box overflow="auto" px={{ base: 2, md: 4 }}>
      <VStack alignItems="stretch" gap={6}>
        {/* All flex items within this VStack should specify an increasing order. This
        will be used by third parties plugins to position themselves within the page via CSS */}
        {alerts.length > 0 ? (
          <Accordion.Root collapsible defaultValue={["ui_alerts"]} order={1}>
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
        <Heading order={2} size="2xl">
          {typeof instanceName === "string" && instanceName !== "" && instanceName !== "Airflow"
            ? instanceName
            : translate("welcome")}
        </Heading>
        <Box order={3}>
          <Stats />
        </Box>
        <Box order={4}>
          <FavoriteDags />
        </Box>
        <Box display="flex" flexDirection={{ base: "column", md: "row" }} gap={{ base: 4, md: 8 }} order={5}>
          <Health />
          <PoolSummary />
        </Box>
        <Box order={6}>
          <TimeRangeSelector
            defaultValue={defaultHour}
            endDate={endDate}
            setEndDate={setEndDate}
            setStartDate={setStartDate}
            startDate={startDate}
          />
        </Box>
        <Box order={7}>
          <DashboardDeadlines endDate={endDate} startDate={startDate} />
        </Box>
        <Box order={8}>
          <HistoricalMetrics endDate={endDate} startDate={startDate} />
        </Box>
        {dashboardReactPlugins.map((plugin) => (
          <ReactPlugin key={plugin.name} reactApp={plugin} />
        ))}
      </VStack>
    </Box>
  );
};
