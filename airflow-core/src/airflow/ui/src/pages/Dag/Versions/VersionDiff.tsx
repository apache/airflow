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
import { Badge, Box, Heading, Text } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { LuInfo } from "react-icons/lu";

import type { DagVersionDiffChangeResponse, DagVersionDiffResponse } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import type { MetaColumn } from "src/components/DataTable/types";

import { DiffUnavailableAlert } from "./DiffUnavailableAlert";
import { DiffValue } from "./DiffValue";
import { PathText } from "./PathText";

type VersionDiffProps = {
  readonly baseVersionNumber: number;
  readonly diff: DagVersionDiffResponse;
  readonly targetVersionNumber: number;
};

const IMPACT_COLORS: Record<string, string> = {
  authorization: "orange",
  execution: "red",
  metadata: "blue",
  provenance: "gray",
  unknown: "gray",
};

type ChangeColumns = Array<MetaColumn<DagVersionDiffChangeResponse>>;

const buildValueColumns = (translate: TFunction<"dag">): ChangeColumns => [
  {
    accessorKey: "before_value",
    // Read off the record rather than the accessor value, so a key the server omitted stays
    // distinguishable from one holding a stored null.
    cell: ({ row }) => <DiffValue value={row.original.before_value} />,
    enableSorting: false,
    header: translate("versions.columns.before"),
  },
  {
    accessorKey: "after_value",
    cell: ({ row }) => <DiffValue value={row.original.after_value} />,
    enableSorting: false,
    header: translate("versions.columns.after"),
  },
];

const buildColumns = (translate: TFunction<"dag">, valuesShown: boolean): ChangeColumns => [
  {
    accessorKey: "path",
    cell: ({ row }) => <PathText path={row.original.path} />,
    enableSorting: false,
    header: translate("versions.columns.path"),
  },
  {
    accessorKey: "operation",
    cell: ({ row }) =>
      translate(`versions.operations.${row.original.operation}`, {
        defaultValue: row.original.operation,
      }),
    enableSorting: false,
    header: translate("versions.columns.operation"),
  },
  {
    accessorKey: "category",
    cell: ({ row }) =>
      translate(`versions.categories.${row.original.category}`, {
        defaultValue: row.original.category,
      }),
    enableSorting: false,
    header: translate("versions.columns.category"),
  },
  {
    accessorKey: "impact",
    cell: ({ row }) => (
      <Badge colorPalette={IMPACT_COLORS[row.original.impact] ?? "gray"}>
        {translate(`versions.impacts.${row.original.impact}`, { defaultValue: row.original.impact })}
      </Badge>
    ),
    enableSorting: false,
    header: translate("versions.columns.impact"),
  },
  {
    accessorKey: "occurrence_count",
    enableSorting: false,
    header: translate("versions.columns.occurrences"),
  },
  ...(valuesShown ? buildValueColumns(translate) : []),
];

export const VersionDiff = ({ baseVersionNumber, diff, targetVersionNumber }: VersionDiffProps) => {
  const { t: translate } = useTranslation("dag");
  const valuesShown = diff.values_status === "available";
  const schemaChanged = diff.serializer_versions.base !== diff.serializer_versions.target;

  if (diff.mode === "unavailable") {
    return <DiffUnavailableAlert reason={diff.unavailable_reason} />;
  }

  return (
    <DataTable
      columns={buildColumns(translate, valuesShown)}
      data={diff.changes}
      // The summary heads the table's own header row, so the comparison, what it leaves out and
      // the control over its columns read as one line rather than three stacked ones.
      headingExtra={
        <Box>
          <Heading size="md">
            {translate("versions.heading", { base: baseVersionNumber, target: targetVersionNumber })}
          </Heading>
          <Text color="fg.muted" fontSize="sm">
            {/* A truncated count is a lower bound, so it says "at least" rather than a bare number. */}
            {translate(diff.truncated ? "versions.summaryAtLeast" : "versions.summary", {
              count: diff.total_changes,
            })}
            {/* Only worth a mention when the two versions were stored by different serializers. */}
            {schemaChanged
              ? ` · ${translate("versions.schemaChange", {
                  baseSchema: diff.serializer_versions.base ?? "?",
                  targetSchema: diff.serializer_versions.target ?? "?",
                })}`
              : undefined}
          </Text>
        </Box>
      }
      // The summary above already counts the changes. A second "N Records" count only invites a
      // comparison that does not hold: a redacted record stands for every change sharing its path.
      hideRowCountHeading
      modelName="dag:versions.record"
      noRowsMessage={translate("versions.noChanges")}
      presentationActions={
        <>
          {/* The hover says what grants values, since the viewer cannot grant it here: the server
                decides from the caller's access to Dag code. */}
          {valuesShown ? undefined : (
            <Tooltip content={translate("versions.codeAccess")} portalled>
              <Badge colorPalette="gray">{translate("versions.valuesHidden")}</Badge>
            </Tooltip>
          )}
          {/* Above the table rather than below it: a long diff would otherwise push the caveat
                off-screen exactly when it matters most. */}
          <Tooltip content={translate("versions.observedStateBoundary")} portalled>
            <Box aria-label={translate("versions.aboutThisComparison")} as="button" color="fg.muted" p={1}>
              <LuInfo />
            </Box>
          </Tooltip>
        </>
      }
      total={diff.changes.length}
    />
  );
};
