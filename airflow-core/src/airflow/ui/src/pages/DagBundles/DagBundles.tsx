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

import { useDagBundleServiceGetDagBundles } from "openapi/queries";
import type { DagBundleResponse } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { ErrorAlert } from "src/components/ErrorAlert";
import { TeamName } from "src/components/TeamName";
import Time from "src/components/Time";

import { useConfig } from "src/queries/useConfig";
import { type DurationFormat, useDocumentTitle, useDurationFormat } from "src/utils";
import { useAutoRefresh } from "src/utils/query";

// A bundle row changes at most once per the bundle's `refresh_interval` (default 300s), so
// `[api] auto_refresh_interval` (default 3s, tuned for Grid/Graph run state) is far too fast here.
// Floor it rather than ignore it, so turning auto-refresh off still turns this page off.
const MIN_REFETCH_INTERVAL_MS = 10_000;

const SHORT_VERSION_LENGTH = 7;

type BundleRow = { row: { original: DagBundleResponse } };

const createColumns = (
  translate: TFunction,
  formatRelative: DurationFormat["formatRelative"],
  multiTeam: boolean,
): Array<ColumnDef<DagBundleResponse>> => [
  {
    accessorKey: "name",
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
    cell: ({ row: { original } }: BundleRow) => {
      if (original.version === null) {
        // A versioning-capable bundle also reports null until its first successful refresh, so an
        // absent last_refreshed is what separates "not refreshed yet" from "not versioned".
        return (
          <Text color="fg.muted">
            {original.last_refreshed === null
              ? translate("browse:dagBundles.notRefreshedYet")
              : translate("browse:dagBundles.notVersioned")}
          </Text>
        );
      }

      // A git bundle stores the full 40-char hexsha, so show the prefix an author recognises and
      // keep the whole value on hover.
      const short = original.version.slice(0, SHORT_VERSION_LENGTH);

      return (
        <Tooltip content={original.version}>
          {original.bundle_url === null ? (
            <Text fontFamily="mono">{short}</Text>
          ) : (
            <Link
              color="fg.info"
              fontFamily="mono"
              href={original.bundle_url}
              rel="noreferrer"
              target="_blank"
            >
              {short}
            </Link>
          )}
        </Tooltip>
      );
    },
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
    cell: ({ row: { original } }: BundleRow) => {
      // null means the user may not read import errors, which is not the same as "none".
      if (original.import_error_count === null) {
        return <Text color="fg.muted">-</Text>;
      }

      return original.import_error_count === 0 ? (
        <Text color="fg.muted">0</Text>
      ) : (
        <Badge colorPalette="failed" variant="solid">
          {original.import_error_count}
        </Badge>
      );
    },
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

  // `useAutoRefresh` with no dagId reduces to `[api] auto_refresh_interval`, where 0 is how an
  // operator turns auto-refresh off -- so it has to short-circuit before the floor below.
  const configuredInterval = useAutoRefresh({});
  const refetchInterval =
    configuredInterval === false || configuredInterval === 0
      ? false
      : Math.max(configuredInterval, MIN_REFETCH_INTERVAL_MS);

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
        modelName="browse:dagBundles.bundle"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
