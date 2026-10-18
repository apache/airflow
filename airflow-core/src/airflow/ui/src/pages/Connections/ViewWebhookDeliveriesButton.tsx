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
import { useTranslation } from "react-i18next";
import { FiInbox } from "react-icons/fi";
import { Link as RouterLink } from "react-router-dom";

import type { ConnectionResponse } from "openapi/requests/types.gen";

import { IconButton } from "src/system-components";

// The webhook <name> is served by the connection webhook_<name> of this type, as
// airflow.webhooks.connections defines them. The name follows
// airflow.webhooks.registry.WEBHOOK_NAME_PATTERN; the API answers 404 for any other.
const WEBHOOK_CONN_ID_PREFIX = "webhook_";
const WEBHOOK_CONN_TYPE = "inbound_webhook";
const WEBHOOK_NAME_PATTERN = /^[a-z0-9][a-z0-9_-]{0,63}$/u;

type Props = {
  readonly connection: ConnectionResponse;
};

const ViewWebhookDeliveriesButton = ({ connection }: Props) => {
  const { t: translate } = useTranslation("browse");

  const webhookName = connection.connection_id.slice(WEBHOOK_CONN_ID_PREFIX.length);

  if (
    connection.conn_type !== WEBHOOK_CONN_TYPE ||
    !connection.connection_id.startsWith(WEBHOOK_CONN_ID_PREFIX) ||
    !WEBHOOK_NAME_PATTERN.test(webhookName)
  ) {
    return undefined;
  }

  return (
    <IconButton asChild label={translate("webhookDeliveries.view")}>
      <RouterLink to={`/webhook_deliveries/${webhookName}`}>
        <FiInbox />
      </RouterLink>
    </IconButton>
  );
};

export default ViewWebhookDeliveriesButton;
