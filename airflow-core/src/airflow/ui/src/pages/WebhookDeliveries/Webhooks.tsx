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
import { Box, Heading, Link, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import { useWebhookServiceGetWebhooks } from "openapi/queries";
import type {
  WebhookConnectionStatus,
  WebhookResponse,
  WebhookSourceTypeStatus,
} from "openapi/requests/types.gen";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import Time from "src/components/Time";

import { useDocumentTitle } from "src/utils";

import { getOutcomeLabel } from "./outcomes";

type StatusText = { readonly explanation?: string; readonly label: string };

// Only a status that keeps the webhook from being served is explained.
const connectionStatusTexts: Record<WebhookConnectionStatus, StatusText> = {
  misconfigured: {
    explanation: "webhooks.connectionStatusExplanations.misconfigured",
    label: "webhooks.connectionStatuses.misconfigured",
  },
  not_found: {
    explanation: "webhooks.connectionStatusExplanations.not_found",
    label: "webhooks.connectionStatuses.not_found",
  },
  ok: { label: "webhooks.connectionStatuses.ok" },
  unavailable: {
    explanation: "webhooks.connectionStatusExplanations.unavailable",
    label: "webhooks.connectionStatuses.unavailable",
  },
  wrong_type: {
    explanation: "webhooks.connectionStatusExplanations.wrong_type",
    label: "webhooks.connectionStatuses.wrong_type",
  },
};

// A source type that is served shows only its name.
const sourceTypeStatusTexts: Record<Exclude<WebhookSourceTypeStatus, "ok">, StatusText> = {
  conflict: {
    explanation: "webhooks.sourceTypeStatusExplanations.conflict",
    label: "webhooks.sourceTypeStatuses.conflict",
  },
  unknown: {
    explanation: "webhooks.sourceTypeStatusExplanations.unknown",
    label: "webhooks.sourceTypeStatuses.unknown",
  },
};

const Explanation = ({ translationKey }: { readonly translationKey?: string }) => {
  const { t: translate } = useTranslation("browse");

  return translationKey === undefined ? undefined : (
    <Text color="fg.muted" fontSize="xs">
      {translate(translationKey)}
    </Text>
  );
};

const createColumns = (translate: TFunction): Array<ColumnDef<DataTableFeatures, WebhookResponse>> => [
  {
    accessorKey: "name",
    cell: ({ row: { original } }) => (
      <Link asChild color="fg.info">
        <RouterLink to={`/webhook_deliveries/${original.name}`}>{original.name}</RouterLink>
      </Link>
    ),
    enableSorting: false,
    header: translate("webhooks.columns.name"),
  },
  {
    accessorKey: "listed",
    cell: ({ row: { original } }) => (
      <Box>
        <Text>{translate(original.listed ? "common:yes" : "common:no")}</Text>
        <Explanation translationKey={original.listed ? undefined : "webhooks.notListed"} />
      </Box>
    ),
    enableSorting: false,
    header: translate("webhooks.columns.listed"),
  },
  {
    accessorKey: "connection_status",
    cell: ({ row: { original } }) => {
      const { explanation, label } = connectionStatusTexts[original.connection_status];

      return (
        <Box>
          <Text>{translate(label)}</Text>
          <Explanation translationKey={explanation} />
        </Box>
      );
    },
    enableSorting: false,
    header: translate("webhooks.columns.connection"),
  },
  {
    accessorKey: "source_type",
    cell: ({ row: { original } }) => {
      const status = original.source_type_status;
      const statusText = status === null || status === "ok" ? undefined : sourceTypeStatusTexts[status];

      return (
        <Box>
          <Text>{original.source_type}</Text>
          {statusText === undefined ? undefined : (
            <>
              <Text>{translate(statusText.label)}</Text>
              <Explanation translationKey={statusText.explanation} />
            </>
          )}
        </Box>
      );
    },
    enableSorting: false,
    header: translate("webhooks.columns.sourceType"),
  },
  {
    accessorKey: "last_received_at",
    cell: ({ row: { original } }) => <Time datetime={original.last_received_at} />,
    enableSorting: false,
    header: translate("webhooks.columns.lastReceivedAt"),
  },
  {
    accessorKey: "last_outcome",
    cell: ({ row: { original } }) =>
      original.last_outcome === null ? undefined : getOutcomeLabel(translate, original.last_outcome),
    enableSorting: false,
    header: translate("webhooks.columns.lastOutcome"),
  },
];

// The Webhooks tab of the deliveries page: each webhook the user may read, whether it can be served,
// and its last delivery.
export const Webhooks = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const title = translate("webhooks.title");

  useDocumentTitle(title);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;

  const { data, error, isLoading } = useWebhookServiceGetWebhooks({
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
  });

  return (
    <Box p={2}>
      <Heading size="lg">{title}</Heading>
      <DataTable
        columns={createColumns(translate)}
        data={data?.webhooks ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="browse:webhooks.webhook"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
