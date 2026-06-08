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
import { Box, Button, Flex, Heading, VStack } from "@chakra-ui/react";
import type { AccordionValueChangeDetails } from "@chakra-ui/react";
import { Fragment } from "react";
import { useTranslation } from "react-i18next";
import { useLocalStorage } from "usehooks-ts";

import { usePluginServiceGetPlugins } from "openapi/queries";
import type { ReactAppResponse, UIAlert } from "openapi/requests/types.gen";
import { Accordion } from "src/components/ui";
import { COLLAPSED_UI_ALERTS_KEY } from "src/constants/localStorage";
import { useConfig } from "src/queries/useConfig";

import { ReactPlugin } from "../ReactPlugin";
import { AlertContent } from "./AlertContent";
import { FavoriteDags } from "./FavoriteDags";
import { Health } from "./Health";
import { HistoricalMetrics } from "./HistoricalMetrics";
import { PoolSummary } from "./PoolSummary";
import { Stats } from "./Stats";

export const Dashboard = () => {
  const alerts = useConfig("dashboard_alert") as Array<UIAlert>;
  const { t: translate } = useTranslation("dashboard");
  const instanceName = useConfig("instance_name");

  const [isCollapsed, setIsCollapsed] = useLocalStorage(COLLAPSED_UI_ALERTS_KEY, false);

  const accordionValue = isCollapsed ? [] : ["ui_alerts"];
  const hasMultipleAlerts = alerts.length > 1;

  const handleAccordionChange = (details: AccordionValueChangeDetails) => {
    setIsCollapsed(!details.value.includes("ui_alerts"));
  };

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
          <Accordion.Root
            collapsible
            data-testid="dashboard-alerts"
            onValueChange={handleAccordionChange}
            order={1}
            value={accordionValue}
          >
            <Accordion.Item key="ui_alerts" value="ui_alerts">
              {alerts.map((alert: UIAlert, index) =>
                index === 0 ? (
                  <Fragment key={alert.text}>
                    {hasMultipleAlerts ? (
                      <Accordion.ItemTrigger mb={2}>
                        <AlertContent alert={alert} />
                      </Accordion.ItemTrigger>
                    ) : (
                      <Box mb={2}>
                        <AlertContent alert={alert} />
                      </Box>
                    )}
                    {isCollapsed && hasMultipleAlerts ? (
                      <Flex justifyContent="center" mb={2}>
                        <Button
                          _hover={{ textDecoration: "underline" }}
                          color="fg.muted"
                          onClick={() => setIsCollapsed(false)}
                          size="sm"
                          variant="plain"
                        >
                          {translate("alerts.showMoreAlerts", { count: alerts.length - 1 })}
                        </Button>
                      </Flex>
                    ) : undefined}
                  </Fragment>
                ) : (
                  <Accordion.ItemContent key={alert.text} pr={8}>
                    <AlertContent alert={alert} />
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
          <HistoricalMetrics />
        </Box>
        {dashboardReactPlugins.map((plugin) => (
          <ReactPlugin key={plugin.name} reactApp={plugin} />
        ))}
      </VStack>
    </Box>
  );
};
