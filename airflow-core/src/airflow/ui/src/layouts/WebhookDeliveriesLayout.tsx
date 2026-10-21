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
import type { PropsWithChildren } from "react";

import { Box } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { NavTabs } from "./Details/NavTabs";

// The tabs of Browse > Webhook Deliveries. The Webhooks tab is not under /webhook_deliveries, where
// any other segment names a webhook, nor under /webhooks, which the API server keeps for inbound
// webhooks.
export const WebhookDeliveriesLayout = ({ children }: PropsWithChildren) => {
  const { t: translate } = useTranslation("browse");

  const tabs = [
    { label: translate("webhookDeliveries.tabs.deliveries"), value: "/webhook_deliveries" },
    { label: translate("webhookDeliveries.tabs.webhooks"), value: "/webhook_overview" },
  ];

  return (
    <Box>
      <NavTabs tabs={tabs} />
      {children}
    </Box>
  );
};
