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
import { Alert, Badge, Box, Code, Flex, Heading, Text } from "@chakra-ui/react";
import type { TFunction } from "i18next";
import { useTranslation } from "react-i18next";
import { LuInfo } from "react-icons/lu";

import type { DagVersionDiffChangeResponse, DagVersionDiffResponse } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import { DataTable } from "src/components/DataTable";
import type { MetaColumn } from "src/components/DataTable/types";

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

// A whole task added or removed carries its entire serialized payload, which would otherwise
// unfold into one unreadable table cell.
const MAX_VALUE_LENGTH = 120;

const renderValue = (value: unknown) => {
  if (value === undefined) {
    return "—";
  }

  // A stored null is a value; only an absent side gets the dash above. An empty string is quoted
  // so it reads as a value rather than as a blank cell.
  const text = typeof value === "string" ? value || '""' : JSON.stringify(value);

  return text.length > MAX_VALUE_LENGTH ? `${text.slice(0, MAX_VALUE_LENGTH)}…` : text;
};

type ChangeColumns = Array<MetaColumn<DagVersionDiffChangeResponse>>;

const buildValueColumns = (translate: TFunction<"dag">): ChangeColumns => [
  {
    accessorKey: "before_value",
    // Read off the record rather than the accessor value, so a key the server omitted stays
    // distinguishable from one holding a stored null.
    cell: ({ row }) => <Code fontSize="sm">{renderValue(row.original.before_value)}</Code>,
    enableSorting: false,
    header: translate("versions.columns.before"),
  },
  {
    accessorKey: "after_value",
    cell: ({ row }) => <Code fontSize="sm">{renderValue(row.original.after_value)}</Code>,
    enableSorting: false,
    header: translate("versions.columns.after"),
  },
];

const buildColumns = (translate: TFunction<"dag">, valuesShown: boolean): ChangeColumns => [
  {
    accessorKey: "path",
    cell: ({ row }) => <Code fontSize="sm">{row.original.path}</Code>,
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

  if (diff.mode === "unavailable") {
    return (
      <Alert.Root status="info" title={translate("versions.unavailable.title")}>
        <Alert.Description>
          {diff.unavailable_reason === null || diff.unavailable_reason === undefined ? (
            translate("versions.unavailable.withoutReason")
          ) : (
            <>
              {translate("versions.unavailable.description")}{" "}
              {/* A machine token, so it is shown as code rather than folded into a translated sentence. */}
              <Code fontSize="sm">{diff.unavailable_reason}</Code>
            </>
          )}
        </Alert.Description>
      </Alert.Root>
    );
  }

  return (
    <Box>
      <Flex alignItems="center" gap={3} justifyContent="space-between" mb={2}>
        <Box>
          <Heading size="md">
            {translate("versions.heading", { base: baseVersionNumber, target: targetVersionNumber })}
          </Heading>
          <Text color="fg.muted" fontSize="sm">
            {translate("versions.summary", {
              baseSchema: diff.serializer_versions.base ?? "?",
              count: diff.total_changes,
              targetSchema: diff.serializer_versions.target ?? "?",
            })}
          </Text>
        </Box>
        <Flex alignItems="center" gap={2}>
          <Badge colorPalette="teal">{translate("versions.observedState")}</Badge>
          <Badge colorPalette={valuesShown ? "green" : "gray"}>
            {translate(valuesShown ? "versions.valuesShown" : "versions.valuesHidden")}
          </Badge>
          {/* Says what grants values, since the viewer cannot grant it here: the server decides
              from the caller's access to Dag code. */}
          <Text color="fg.muted" fontSize="sm">
            {translate("versions.codeAccess")}
          </Text>
          {/* Above the table rather than below it: a long diff would otherwise push the caveat
              off-screen exactly when it matters most. */}
          <Tooltip content={translate("versions.observedStateBoundary")} portalled>
            <Box aria-label={translate("versions.aboutThisComparison")} as="button" color="fg.muted" p={1}>
              <LuInfo />
            </Box>
          </Tooltip>
        </Flex>
      </Flex>

      {diff.truncated ? (
        <Alert.Root mb={2} status="warning" title={translate("versions.truncated.title")}>
          <Alert.Description>{translate("versions.truncated.description")}</Alert.Description>
        </Alert.Root>
      ) : undefined}

      <DataTable
        columns={buildColumns(translate, valuesShown)}
        data={diff.changes}
        modelName="dag:versions.record"
        noRowsMessage={translate("versions.noChanges")}
        // Records, not underlying changes: a redacted record stands for every change sharing its
        // path, which is why this can read lower than the count in the summary above.
        total={diff.changes.length}
      />
    </Box>
  );
};
