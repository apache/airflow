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

import { Table, Text } from "@chakra-ui/react";
import type { FC } from "react";

import { RuleStatusBadge } from "src/components/RuleStatusBadge";
import type { RuleResultRecord } from "src/types/dq";

function formatCondition(condition: Record<string, number>): string {
  return Object.entries(condition)
    .map(([key, value]) => `${key}=${value}`)
    .join(", ");
}

interface RuleResultsTableProps {
  results: Array<RuleResultRecord>;
}

export const RuleResultsTable: FC<RuleResultsTableProps> = ({ results }) => (
  <Table.Root size="sm" striped>
    <Table.Body>
      <Table.Row>
        <Table.ColumnHeader>Rule</Table.ColumnHeader>
        <Table.ColumnHeader>Status</Table.ColumnHeader>
        <Table.ColumnHeader>Dimension</Table.ColumnHeader>
        <Table.ColumnHeader>Severity</Table.ColumnHeader>
        <Table.ColumnHeader>Observed Value</Table.ColumnHeader>
        <Table.ColumnHeader>Condition</Table.ColumnHeader>
        <Table.ColumnHeader>Duration</Table.ColumnHeader>
      </Table.Row>
      {results.map((result) => (
        <Table.Row key={result.rule_uid}>
          <Table.Cell>
            <Text fontWeight="medium">{result.rule_name}</Text>
            {result.error_message ? (
              <Text color="fg.error" fontSize="xs">
                {result.error_message}
              </Text>
            ) : null}
          </Table.Cell>
          <Table.Cell>
            <RuleStatusBadge status={result.status} />
          </Table.Cell>
          <Table.Cell>{result.dimension}</Table.Cell>
          <Table.Cell>{result.severity}</Table.Cell>
          <Table.Cell>{result.observed_value ?? "—"}</Table.Cell>
          <Table.Cell>{formatCondition(result.condition)}</Table.Cell>
          <Table.Cell>{result.duration_ms === null ? "—" : `${result.duration_ms.toFixed(0)} ms`}</Table.Cell>
        </Table.Row>
      ))}
    </Table.Body>
  </Table.Root>
);
