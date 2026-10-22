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
import { Box, Heading, Link } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { LuPanelRightOpen } from "react-icons/lu";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useWebhookServiceGetWebhookDeliveries } from "openapi/queries";
import type { WebhookDeliveryResponse } from "openapi/requests/types.gen";

import { IconButton } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import Time from "src/components/Time";

import { SearchParamsKeys } from "src/constants/searchParams";
import { useDocumentTitle, useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

import { DeliveryDetailsDrawer } from "./DeliveryDetailsDrawer";
import { getOutcomeLabel } from "./outcomes";

// The deliveries API reads "~" as every webhook the caller may read.
const ALL_WEBHOOKS = "~";

const createColumns = ({
  onShowDetails,
  showWebhook,
  translate,
}: {
  onShowDetails: (id: string) => void;
  showWebhook: boolean;
  translate: TFunction;
}): Array<ColumnDef<DataTableFeatures, WebhookDeliveryResponse>> => [
  {
    accessorKey: "received_at",
    cell: ({ row: { original } }) => <Time datetime={original.received_at} />,
    header: translate("webhookDeliveries.columns.receivedAt"),
  },
  ...(showWebhook
    ? [
        {
          accessorKey: "webhook",
          cell: ({ row: { original } }: { row: { original: WebhookDeliveryResponse } }) => (
            <Link asChild color="fg.info">
              <RouterLink to={`/webhook_deliveries/${original.webhook}`}>{original.webhook}</RouterLink>
            </Link>
          ),
          header: translate("webhookDeliveries.columns.webhook"),
        },
      ]
    : []),
  {
    accessorKey: "outcome",
    cell: ({ row: { original } }) => getOutcomeLabel(translate, original.outcome),
    header: translate("webhookDeliveries.columns.outcome"),
  },
  {
    accessorKey: "delivery_id",
    header: translate("webhookDeliveries.columns.deliveryId"),
  },
  {
    accessorKey: "correlation_key",
    enableSorting: false,
    header: translate("webhookDeliveries.columns.correlationKey"),
  },
  {
    // Recorded per delivery: rows received before the connection's source type changed keep the old one.
    accessorKey: "source_type",
    enableSorting: false,
    header: translate("webhookDeliveries.columns.sourceType"),
  },
  {
    accessorKey: "details",
    cell: ({ row: { original } }) => (
      <IconButton
        label={translate("webhookDeliveries.details.show")}
        onClick={() => onShowDetails(original.id)}
        size="xs"
      >
        <LuPanelRightOpen />
      </IconButton>
    ),
    enableHiding: false,
    enableSorting: false,
    header: "",
  },
];

const deliveryFilterKeys: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.RECEIVED_AT_RANGE,
  SearchParamsKeys.OUTCOME,
  SearchParamsKeys.DELIVERY_ID,
  SearchParamsKeys.CORRELATION_KEY,
  SearchParamsKeys.SOURCE_TYPE,
];

// Both /webhook_deliveries (every webhook) and /webhook_deliveries/:webhookName render this page.
export const WebhookDeliveries = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const { webhookName } = useParams();
  const showsAllWebhooks = webhookName === undefined;
  const title = showsAllWebhooks
    ? translate("common:browse.webhookDeliveries")
    : translate("webhookDeliveries.title", { webhookName });

  useDocumentTitle(title);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { filterConfigs, handleFiltersChange, initialValues, searchParams } = useFiltersHandler(
    showsAllWebhooks ? [SearchParamsKeys.WEBHOOK_NAME, ...deliveryFilterKeys] : deliveryFilterKeys,
  );
  const [, setSearchParams] = useSearchParams();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : undefined;
  const webhookNames = searchParams.getAll(SearchParamsKeys.WEBHOOK_NAME).filter((name) => name !== "");
  const selectedDeliveryId = searchParams.get(SearchParamsKeys.ID) ?? undefined;

  const { data, error, isLoading } = useWebhookServiceGetWebhookDeliveries({
    correlationKey: searchParams.get(SearchParamsKeys.CORRELATION_KEY) ?? undefined,
    deliveryId: searchParams.get(SearchParamsKeys.DELIVERY_ID) ?? undefined,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    outcome: searchParams.get(SearchParamsKeys.OUTCOME) ?? undefined,
    receivedAtGte: searchParams.get(SearchParamsKeys.RECEIVED_AT_GTE) ?? undefined,
    receivedAtLte: searchParams.get(SearchParamsKeys.RECEIVED_AT_LTE) ?? undefined,
    sourceType: searchParams.get(SearchParamsKeys.SOURCE_TYPE) ?? undefined,
    webhookName: webhookName ?? ALL_WEBHOOKS,
    webhookNames: showsAllWebhooks && webhookNames.length > 0 ? webhookNames : undefined,
  });

  const setSelectedDeliveryId = (id?: string) =>
    setSearchParams((previous) => {
      const next = new URLSearchParams(previous);

      if (id === undefined) {
        next.delete(SearchParamsKeys.ID);
      } else {
        next.set(SearchParamsKeys.ID, id);
      }

      return next;
    });

  return (
    <Box p={2}>
      <Heading size="lg">{title}</Heading>
      <DataTable
        columns={createColumns({
          onShowDetails: setSelectedDeliveryId,
          showWebhook: showsAllWebhooks,
          translate,
        })}
        data={data?.deliveries ?? []}
        errorMessage={<ErrorAlert error={error} />}
        filterActions={
          <FilterBar
            configs={filterConfigs}
            initialValues={initialValues}
            onFiltersChange={handleFiltersChange}
          />
        }
        initialState={tableURLState}
        isLoading={isLoading}
        modelName="browse:webhookDeliveries.delivery"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
      <DeliveryDetailsDrawer
        deliveryId={selectedDeliveryId}
        onClose={() => setSelectedDeliveryId(undefined)}
        webhookName={webhookName ?? ALL_WEBHOOKS}
      />
    </Box>
  );
};
