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
import { Box, Heading } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useWebhookServiceGetWebhookDeliveries } from "openapi/queries";
import type { WebhookDeliveryResponse } from "openapi/requests/types.gen";

import { DataTable } from "src/components/DataTable";
import type { DataTableFeatures } from "src/components/DataTable/features";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { FilterBar } from "src/components/FilterBar";
import RenderedJsonField from "src/components/RenderedJsonField";
import Time from "src/components/Time";

import { SearchParamsKeys } from "src/constants/searchParams";
import { webhookDeliveryOutcomeOptions } from "src/constants/stateOptions";
import { useDocumentTitle, useFiltersHandler, type FilterableSearchParamsKeys } from "src/utils";

// A delivery that only recorded topic events shows their names and ids, not a lone `asset_events: {…}`.
const getActionsPreview = (actions: NonNullable<WebhookDeliveryResponse["actions"]>): object => {
  const { asset_events: assetEvents, ...otherActions } = actions;

  return Object.keys(otherActions).length === 0 && typeof assetEvents === "object" && assetEvents !== null
    ? assetEvents
    : actions;
};

const createColumns = (
  translate: TFunction,
): Array<ColumnDef<DataTableFeatures, WebhookDeliveryResponse>> => [
  {
    accessorKey: "received_at",
    cell: ({ row: { original } }) => <Time datetime={original.received_at} />,
    header: translate("webhookDeliveries.columns.receivedAt"),
  },
  {
    accessorKey: "delivery_id",
    header: translate("webhookDeliveries.columns.deliveryId"),
  },
  {
    accessorKey: "outcome",
    cell: ({ row: { original } }) => {
      const option = webhookDeliveryOutcomeOptions.items.find((item) => item.value === original.outcome);

      return option === undefined ? original.outcome : translate(option.label);
    },
    header: translate("webhookDeliveries.columns.outcome"),
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
    accessorKey: "attributes",
    cell: ({ row: { original } }) =>
      original.attributes === null ? undefined : (
        <RenderedJsonField collapsed content={original.attributes} />
      ),
    enableSorting: false,
    header: translate("webhookDeliveries.columns.attributes"),
  },
  {
    accessorKey: "candidates",
    cell: ({ row: { original } }) =>
      original.candidates === null ? undefined : (
        <RenderedJsonField collapsed content={original.candidates} />
      ),
    enableSorting: false,
    header: translate("webhookDeliveries.columns.candidates"),
  },
  {
    accessorKey: "actions",
    cell: ({ row: { original } }) =>
      original.actions === null ? undefined : (
        <RenderedJsonField collapsed content={getActionsPreview(original.actions)} />
      ),
    enableSorting: false,
    header: translate("webhookDeliveries.columns.actions"),
  },
];

const webhookDeliveriesFilterKeys: Array<FilterableSearchParamsKeys> = [
  SearchParamsKeys.RECEIVED_AT_RANGE,
  SearchParamsKeys.OUTCOME,
  SearchParamsKeys.DELIVERY_ID,
  SearchParamsKeys.CORRELATION_KEY,
  SearchParamsKeys.ID,
];

export const WebhookDeliveries = () => {
  const { t: translate } = useTranslation("browse");
  // The route segment is required, so react-router cannot match this without a name.
  const { webhookName = "" } = useParams();
  const title = translate("webhookDeliveries.title", { webhookName });

  useDocumentTitle(title);

  const { setTableURLState, tableURLState } = useTableURLState();
  const { filterConfigs, handleFiltersChange, initialValues, searchParams } =
    useFiltersHandler(webhookDeliveriesFilterKeys);
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : undefined;

  const { data, error, isLoading } = useWebhookServiceGetWebhookDeliveries({
    correlationKey: searchParams.get(SearchParamsKeys.CORRELATION_KEY) ?? undefined,
    deliveryId: searchParams.get(SearchParamsKeys.DELIVERY_ID) ?? undefined,
    id: searchParams.get(SearchParamsKeys.ID) ?? undefined,
    limit: pagination.pageSize,
    offset: pagination.pageIndex * pagination.pageSize,
    orderBy,
    outcome: searchParams.get(SearchParamsKeys.OUTCOME) ?? undefined,
    receivedAtGte: searchParams.get(SearchParamsKeys.RECEIVED_AT_GTE) ?? undefined,
    receivedAtLte: searchParams.get(SearchParamsKeys.RECEIVED_AT_LTE) ?? undefined,
    webhookName,
  });

  return (
    <Box p={2}>
      <Heading size="lg">{title}</Heading>
      <DataTable
        columns={createColumns(translate)}
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
    </Box>
  );
};
