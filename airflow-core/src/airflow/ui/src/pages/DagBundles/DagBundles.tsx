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
import { Badge, Box, Link, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { Link as RouterLink } from "react-router-dom";

import { useDagBundleServiceGetDagBundles } from "openapi/queries";
import type { DagBundleResponse } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import { DagBundleVersion } from "src/components/DagBundleVersion";
import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ImportErrorCount } from "src/components/ImportErrorCount";
import { TeamName } from "src/components/TeamName";
import Time from "src/components/Time";

import { useConfig } from "src/queries/useConfig";
import { useDagBundleRefetchInterval } from "src/queries/useDagBundleRefetchInterval";
import { type DurationFormat, useDocumentTitle, useDurationFormat } from "src/utils";

type BundleRow = { row: { original: DagBundleResponse } };

const createColumns = (
  translate: TFunction,
  formatRelative: DurationFormat["formatRelative"],
  multiTeam: boolean,
): Array<ColumnDef<DagBundleResponse>> => [
  {
    accessorKey: "name",
    cell: ({ row: { original } }: BundleRow) => (
      <Link asChild color="fg.info" fontWeight="bold">
        <RouterLink to={`/dag_bundles/${encodeURIComponent(original.name)}`}>{original.name}</RouterLink>
      </Link>
    ),
    header: translate("browse:dagBundles.columns.name"),
  },
  ...(multiTeam
    ? [
        {
          accessorKey: "team_name",
          cell: ({ row: { original } }: BundleRow) => <TeamName teamName={original.team_name} />,
          enableSorting: false,
          header: translate("common:dagDetails.team"),
        },
      ]
    : []),
  {
    accessorKey: "version",
    cell: ({ row: { original } }: BundleRow) => (
      <DagBundleVersion
        bundleUrl={original.bundle_url}
        lastRefreshed={original.last_refreshed}
        version={original.version}
      />
    ),
    header: translate("browse:dagBundles.columns.version"),
  },
  {
    accessorKey: "last_refreshed",
    cell: ({ row: { original } }: BundleRow) =>
      original.last_refreshed === null ? (
        <Text color="fg.muted">{translate("browse:dagBundles.neverRefreshed")}</Text>
      ) : (
        <Tooltip content={<Time datetime={original.last_refreshed} showTooltip={false} />}>
          <Text>{formatRelative(original.last_refreshed)}</Text>
        </Tooltip>
      ),
    header: translate("browse:dagBundles.columns.lastRefreshed"),
  },
  {
    accessorKey: "import_error_count",
    cell: ({ row: { original } }: BundleRow) => <ImportErrorCount count={original.import_error_count} />,
    enableSorting: false,
    header: translate("browse:dagBundles.columns.importErrors"),
  },
  {
    accessorKey: "active",
    cell: ({ row: { original } }: BundleRow) =>
      // Spelled out both ways rather than left blank when healthy, since an all-empty column reads
      // as a broken page.
      original.active === true ? (
        <Badge colorPalette="success">{translate("browse:dagBundles.active")}</Badge>
      ) : (
        <Badge colorPalette="gray">{translate("browse:dagBundles.inactive")}</Badge>
      ),
    header: translate("browse:dagBundles.columns.active"),
  },
];

export const DagBundles = () => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const { formatRelative } = useDurationFormat();
  const multiTeamEnabled = Boolean(useConfig("multi_team"));

  useDocumentTitle(translate("common:browse.dagBundles"));

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination, sorting } = tableURLState;
  const [sort] = sorting;
  const orderBy = sort ? [`${sort.desc ? "-" : ""}${sort.id}`] : undefined;

  const refetchInterval = useDagBundleRefetchInterval();

  const { data, error, isFetching, isLoading } = useDagBundleServiceGetDagBundles(
    {
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
      orderBy,
    },
    undefined,
    // "Has my deploy landed yet" is exactly what a returning tab is asking, so refresh on focus
    // too -- unless polling is off altogether.
    { refetchInterval, refetchOnWindowFocus: refetchInterval !== false },
  );

  const columns = createColumns(translate, formatRelative, multiTeamEnabled);

  return (
    <Box p={2}>
      <DataTable
        columns={columns}
        data={data?.dag_bundles ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="common:dagBundle"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
