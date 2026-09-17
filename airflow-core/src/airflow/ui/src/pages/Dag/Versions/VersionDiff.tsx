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
import { Alert, Badge, Box, Code, Flex, Heading, Table, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import type { DagVersionDiffResponse } from "openapi/requests/types.gen";

type VersionDiffProps = {
  readonly baseVersionNumber: number;
  readonly diff: DagVersionDiffResponse;
  readonly targetVersionNumber: number;
};

const IMPACT_COLOURS: Record<string, string> = {
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

  // A stored null is a value; only an absent side gets the dash above.
  const text = typeof value === "string" ? value : JSON.stringify(value);

  return text.length > MAX_VALUE_LENGTH ? `${text.slice(0, MAX_VALUE_LENGTH)}…` : text;
};

export const VersionDiff = ({ baseVersionNumber, diff, targetVersionNumber }: VersionDiffProps) => {
  const { t: translate } = useTranslation("dag");
  const valuesShown = diff.values_status === "available";

  if (diff.mode === "unavailable") {
    return (
      <Alert.Root status="info" title={translate("versions.unavailable.title")}>
        <Alert.Description>
          {translate("versions.unavailable.description")}{" "}
          {/* A machine token, so it is shown as code rather than folded into a translated sentence. */}
          <Code fontSize="sm">{diff.unavailable_reason}</Code>
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
              base: diff.serialized_dag_schema_versions.base ?? "?",
              count: diff.total_changes,
              target: diff.serialized_dag_schema_versions.target ?? "?",
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
        </Flex>
      </Flex>

      {diff.truncated ? (
        <Alert.Root mb={2} status="warning" title={translate("versions.truncated.title")}>
          <Alert.Description>{translate("versions.truncated.description")}</Alert.Description>
        </Alert.Root>
      ) : undefined}

      <Table.Root striped>
        <Table.Header>
          <Table.Row>
            <Table.ColumnHeader>{translate("versions.columns.path")}</Table.ColumnHeader>
            <Table.ColumnHeader>{translate("versions.columns.operation")}</Table.ColumnHeader>
            <Table.ColumnHeader>{translate("versions.columns.category")}</Table.ColumnHeader>
            <Table.ColumnHeader>{translate("versions.columns.impact")}</Table.ColumnHeader>
            <Table.ColumnHeader>{translate("versions.columns.occurrences")}</Table.ColumnHeader>
            {valuesShown ? (
              <>
                <Table.ColumnHeader>{translate("versions.columns.before")}</Table.ColumnHeader>
                <Table.ColumnHeader>{translate("versions.columns.after")}</Table.ColumnHeader>
              </>
            ) : undefined}
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {diff.changes.map((change) => (
            <Table.Row key={`${change.path}-${change.operation}`}>
              <Table.Cell>
                <Code fontSize="sm">{change.path}</Code>
              </Table.Cell>
              <Table.Cell>{change.operation}</Table.Cell>
              <Table.Cell>{change.category}</Table.Cell>
              <Table.Cell>
                <Badge colorPalette={IMPACT_COLOURS[change.impact] ?? "gray"}>{change.impact}</Badge>
              </Table.Cell>
              <Table.Cell>{change.occurrence_count}</Table.Cell>
              {valuesShown ? (
                <>
                  <Table.Cell>
                    <Code fontSize="sm">{renderValue(change.before_value)}</Code>
                  </Table.Cell>
                  <Table.Cell>
                    <Code fontSize="sm">{renderValue(change.after_value)}</Code>
                  </Table.Cell>
                </>
              ) : undefined}
            </Table.Row>
          ))}
        </Table.Body>
      </Table.Root>

      <Text color="fg.muted" fontSize="sm" mt={3}>
        {translate("versions.observedStateBoundary")}
      </Text>
    </Box>
  );
};
