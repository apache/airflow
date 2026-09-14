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
import { useState } from "react";

import { Box, Button, Text } from "@chakra-ui/react";
import type { ColumnDef } from "@tanstack/react-table";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";

import { useDagBundleServiceGetDagBundleFiles } from "openapi/queries";
import type { DagBundleFileResponse } from "openapi/requests/types.gen";

import { DataTable } from "src/components/DataTable";
import { useTableURLState } from "src/components/DataTable/useTableUrlState";
import { DurationCell } from "src/components/DurationCell";
import { ErrorAlert } from "src/components/ErrorAlert";
import { ImportErrorCount } from "src/components/ImportErrorCount";
import Time from "src/components/Time";

import { useDagBundleRefetchInterval } from "src/queries/useDagBundleRefetchInterval";

import { FileImportError } from "./FileImportError";

type FileRow = { row: { original: DagBundleFileResponse } };

// The endpoint assembles rows from two sources and orders them by path, so it takes no sort
// parameter and the table offers none.
const createColumns = (
  translate: TFunction,
  onShowImportError: (relativeFileloc: string) => void,
): Array<ColumnDef<DagBundleFileResponse>> => [
  {
    accessorKey: "relative_fileloc",
    cell: ({ row: { original } }: FileRow) => <Text fontFamily="mono">{original.relative_fileloc}</Text>,
    enableSorting: false,
    header: translate("browse:dagBundles.files.columns.file"),
  },
  {
    accessorKey: "dag_count",
    enableSorting: false,
    header: translate("common:dag_other"),
  },
  {
    accessorKey: "last_parsed_time",
    cell: ({ row: { original } }: FileRow) =>
      original.last_parsed_time === null ? (
        <Text color="fg.muted">{translate("browse:dagBundles.files.neverParsed")}</Text>
      ) : (
        <Time datetime={original.last_parsed_time} />
      ),
    enableSorting: false,
    header: translate("browse:dagBundles.files.columns.lastParsed"),
  },
  {
    accessorKey: "last_parse_duration",
    cell: ({ row: { original } }: FileRow) => <DurationCell duration={original.last_parse_duration} />,
    enableSorting: false,
    header: translate("browse:dagBundles.files.columns.parseDuration"),
  },
  {
    accessorKey: "import_error_count",
    cell: ({ row: { original } }: FileRow) =>
      // Only worth clicking when there is an error behind the count.
      original.import_error_count === null || original.import_error_count === 0 ? (
        <ImportErrorCount count={original.import_error_count} />
      ) : (
        <Button
          aria-label={translate("browse:dagBundles.files.showImportError")}
          onClick={() => {
            onShowImportError(original.relative_fileloc);
          }}
          size="xs"
          variant="plain"
        >
          <ImportErrorCount count={original.import_error_count} />
        </Button>
      ),
    enableSorting: false,
    header: translate("browse:dagBundles.columns.importErrors"),
  },
];

export const BundleFiles = ({ bundleName }: { readonly bundleName: string }) => {
  const { t: translate } = useTranslation(["browse", "common"]);
  const refetchInterval = useDagBundleRefetchInterval();

  const { setTableURLState, tableURLState } = useTableURLState();
  const { pagination } = tableURLState;
  const [errorFileloc, setErrorFileloc] = useState<string | undefined>(undefined);

  const { data, error, isFetching, isLoading } = useDagBundleServiceGetDagBundleFiles(
    {
      bundleName,
      limit: pagination.pageSize,
      offset: pagination.pageIndex * pagination.pageSize,
    },
    undefined,
    { refetchInterval, refetchOnWindowFocus: refetchInterval !== false },
  );

  return (
    <Box pt={2}>
      <FileImportError
        bundleName={bundleName}
        onClose={() => {
          setErrorFileloc(undefined);
        }}
        relativeFileloc={errorFileloc}
      />
      <DataTable
        columns={createColumns(translate, setErrorFileloc)}
        data={data?.dag_bundle_files ?? []}
        errorMessage={<ErrorAlert error={error} />}
        initialState={tableURLState}
        isFetching={isFetching}
        isLoading={isLoading}
        modelName="browse:dagBundles.files.file"
        onStateChange={setTableURLState}
        total={data?.total_entries}
      />
    </Box>
  );
};
