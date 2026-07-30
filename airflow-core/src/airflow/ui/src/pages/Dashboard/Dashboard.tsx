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
import { Box, Button, Flex, Heading, Separator, VStack } from "@chakra-ui/react";
import dayjs from "dayjs";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { LuChevronDown } from "react-icons/lu";
import { useLocalStorage } from "usehooks-ts";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { ReactAppResponse, UIAlert } from "openapi/requests/types.gen";
import TimeRangeSelector from "src/components/TimeRangeSelector";
import { IconButton } from "src/components/ui";
import { COLLAPSED_UI_ALERTS_KEY } from "src/constants/localStorage";
import { useConfig } from "src/queries/useConfig";
import { useDocumentTitle } from "src/utils";

import { ReactPlugin } from "../ReactPlugin";
import { AlertContent } from "./AlertContent";
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

  useDocumentTitle(translate("common:nav.home"));

  const instanceName = useConfig("instance_name");

  const now = dayjs();
  const [startDate, setStartDate] = useState(now.subtract(Number(defaultHour), "hour").toISOString());
  const [endDate, setEndDate] = useState(now.toISOString());
  const [isCollapsed, setIsCollapsed] = useLocalStorage(COLLAPSED_UI_ALERTS_KEY, false);

  const [firstAlert, ...restAlerts] = alerts;
  const hasMultipleAlerts = restAlerts.length > 0;

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
        {firstAlert ? (
          <Box data-testid="dashboard-alerts" order={1}>
            <Box mb={2}>
              <AlertContent alert={firstAlert} />
            </Box>
            {isCollapsed
              ? undefined
              : restAlerts.map((alert: UIAlert) => (
                  <Box key={alert.text} mb={2}>
                    <AlertContent alert={alert} />
                  </Box>
                ))}
            {hasMultipleAlerts ? (
              <>
                {isCollapsed ? (
                  <Flex justifyContent="center" mb={2}>
                    <Button
                      _hover={{ textDecoration: "underline" }}
                      color="fg.muted"
                      onClick={() => setIsCollapsed(false)}
                      size="sm"
                      variant="plain"
                    >
                      {translate("alerts.showMoreAlerts", { count: restAlerts.length })}
                    </Button>
                  </Flex>
                ) : undefined}
                <Flex alignItems="center" gap={2}>
                  <Separator flex="1" />
                  <IconButton
                    aria-label={
                      isCollapsed
                        ? `Show ${restAlerts.length} more alert${restAlerts.length === 1 ? "" : "s"}`
                        : "Show fewer alerts"
                    }
                    onClick={() => setIsCollapsed((previous) => !previous)}
                    size="xs"
                    variant="ghost"
                  >
                    <LuChevronDown
                      style={{
                        transform: isCollapsed ? undefined : "rotate(180deg)",
                        transition: "transform 0.2s",
                      }}
                    />
                  </IconButton>
                  <Separator flex="1" />
                </Flex>
              </>
            ) : undefined}
          </Box>
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
