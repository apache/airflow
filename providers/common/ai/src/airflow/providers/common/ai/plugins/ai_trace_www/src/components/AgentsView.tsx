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

import { Badge, Box, Code, Spinner, Table, Text } from "@chakra-ui/react";
import { type FC, useEffect, useState } from "react";

import { ApiError, fetchAgents } from "src/api";
import type { AgentItem } from "src/types";
import { fmtTimestamp } from "src/util";

// Deployment-wide agent inventory: one row per (dag, task) that ran a
// common.ai operator. Row click drills into that agent's traces.
export const AgentsView: FC<{ onSelect: (dagId: string, taskId: string) => void }> = ({ onSelect }) => {
  const [items, setItems] = useState<AgentItem[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchAgents()
      .then(setItems)
      .catch((err: unknown) => {
        setError(err instanceof ApiError ? err.message : err instanceof Error ? err.message : String(err));
      });
  }, []);

  if (error) {
    return (
      <Box bg="bg.subtle" borderRadius="lg" borderWidth="1px" color="fg.muted" fontSize="sm" p={5}>
        {error}
      </Box>
    );
  }

  if (items === null) {
    return (
      <Box p={5}>
        <Spinner size="sm" />
      </Box>
    );
  }

  if (items.length === 0) {
    return (
      <Box bg="bg.subtle" borderRadius="lg" borderWidth="1px" color="fg.muted" fontSize="sm" p={5}>
        No agents found. Tasks using <Code fontSize="xs">@task.agent</Code> /{" "}
        <Code fontSize="xs">@task.llm</Code> (or the common.ai operators) will show up here.
      </Box>
    );
  }

  return (
    <Table.Root size="sm" striped>
      <Table.Header>
        <Table.Row>
          <Table.ColumnHeader>Agent</Table.ColumnHeader>
          <Table.ColumnHeader>Operator</Table.ColumnHeader>
          <Table.ColumnHeader>Runs</Table.ColumnHeader>
          <Table.ColumnHeader>Failed</Table.ColumnHeader>
          <Table.ColumnHeader>Last run</Table.ColumnHeader>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {items.map((a) => (
          <Table.Row
            _hover={{ bg: "bg.subtle", cursor: "pointer" }}
            key={`${a.dag_id}-${a.task_id}`}
            onClick={() => onSelect(a.dag_id, a.task_id)}
          >
            <Table.Cell>
              <Text fontWeight="medium">
                {a.dag_id}
                <Text as="span" color="fg.muted">
                  .{a.task_id}
                </Text>
              </Text>
            </Table.Cell>
            <Table.Cell>
              <Badge colorPalette="purple" variant="outline">
                {a.operator ?? "?"}
              </Badge>
            </Table.Cell>
            <Table.Cell>
              <Text fontSize="xs">{a.runs}</Text>
            </Table.Cell>
            <Table.Cell>
              {a.failed > 0 ? (
                <Badge colorPalette="red">{a.failed}</Badge>
              ) : (
                <Text color="fg.muted" fontSize="xs">
                  0
                </Text>
              )}
            </Table.Cell>
            <Table.Cell>
              <Text fontSize="xs">{fmtTimestamp(a.last_run)}</Text>
            </Table.Cell>
          </Table.Row>
        ))}
      </Table.Body>
    </Table.Root>
  );
};
