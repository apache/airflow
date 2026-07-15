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

import { Box, Button, Code, Flex, Grid, Link, Spinner, Table, Text } from "@chakra-ui/react";
import { type FC, type ReactNode, useState } from "react";

import { DQErrorState } from "src/components/DQErrorState";
import { RuleStatusBadge } from "src/components/RuleStatusBadge";
import type { RuleHistoryRecord, RuleResultRecord } from "src/types/dq";
import { buildTaskInstanceHref } from "src/utils/taskInstanceLink";

interface RuleHistoryPageProps {
  history: Array<RuleHistoryRecord>;
  loading: boolean;
  loadingMore: boolean;
  nextCursor: string | null;
  onBack: () => void;
  onLoadMore: () => void;
  onRefresh: () => void;
  rule: RuleResultRecord;
  ruleHistoryError: string | null;
}

const SummaryItem: FC<{ label: string; value: ReactNode }> = ({ label, value }) => (
  <Box>
    <Text color="fg.muted" fontSize="xs" mb={1} textTransform="uppercase">
      {label}
    </Text>
    <Box fontSize="sm" fontWeight="medium">
      {value}
    </Box>
  </Box>
);

function formatDate(value: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return value;
  return date.toLocaleString();
}

function formatDuration(value: number | null): string {
  return value === null ? "-" : `${value.toFixed(0)} ms`;
}

function formatCondition(condition: Record<string, number>): string {
  if ("equal_to" in condition) return `= ${condition.equal_to}`;
  if ("geq_to" in condition) return `>= ${condition.geq_to}`;
  if ("greater_than" in condition) return `> ${condition.greater_than}`;
  if ("leq_to" in condition) return `<= ${condition.leq_to}`;
  if ("less_than" in condition) return `< ${condition.less_than}`;
  return Object.entries(condition)
    .map(([key, value]) => `${key}=${value}`)
    .join(", ");
}

function getTriggeredBy(runId: string): string {
  if (runId.startsWith("manual__")) return "manual";
  if (runId.startsWith("scheduled__")) return "scheduler";
  return "-";
}

function getRuleDescription(rule: RuleResultRecord): string {
  return rule.description ?? rule.rule_name.replaceAll("_", " ");
}

export const RuleHistoryPage: FC<RuleHistoryPageProps> = ({
  history,
  loading,
  loadingMore,
  nextCursor,
  onBack,
  onLoadMore,
  onRefresh,
  rule,
  ruleHistoryError,
}) => {
  const [sqlExpanded, setSqlExpanded] = useState(false);
  const latestHistory = history[0];
  const latest = latestHistory ?? rule;
  const sql = latest.sql ?? "SQL was not captured for this run.";

  return (
    <Box p={4}>
      <Flex alignItems="center" justifyContent="space-between" mb={4} wrap="wrap">
        <Button onClick={onBack} size="sm" variant="ghost">
          Back to Data Quality
        </Button>
        <Button onClick={onRefresh} size="sm" variant="outline">
          Refresh
        </Button>
      </Flex>

      <Box mb={4}>
        <Text fontSize="lg" fontWeight="semibold">
          Rule History
        </Text>
        <Text fontSize="xl" fontWeight="semibold" mt={1}>
          {rule.rule_name}
        </Text>
        <Text color="fg.muted" fontSize="sm" mt={1}>
          {getRuleDescription(latest)}
        </Text>
      </Box>

      <Box borderRadius="lg" borderWidth="1px" mb={4} p={4}>
        <Text fontSize="md" fontWeight="semibold" mb={3}>
          Rule Details
        </Text>
        <Grid gap={4} templateColumns="repeat(auto-fit, minmax(140px, 1fr))">
          <SummaryItem label="Rule" value={rule.rule_name} />
          <SummaryItem label="Status" value={<RuleStatusBadge status={latest.status} />} />
          <SummaryItem label="Threshold" value={formatCondition(rule.condition)} />
          <SummaryItem label="Last Value" value={latest.observed_value === null ? "-" : String(latest.observed_value)} />
          <SummaryItem label="Last Evaluated" value={formatDate(latestHistory?.run.started_at ?? null)} />
        </Grid>
      </Box>

      <Box borderRadius="lg" borderWidth="1px" mb={4} p={4}>
        <Text fontSize="md" fontWeight="semibold" mb={3}>
          Execution History
        </Text>
        {ruleHistoryError ? <DQErrorState message={ruleHistoryError} /> : null}
        {loading ? (
          <Box display="flex" justifyContent="center" p={6}>
            <Spinner size="sm" />
          </Box>
        ) : history.length === 0 ? (
          <Box py={6}>
            <Text fontWeight="medium">No execution history available.</Text>
            <Text color="fg.muted" fontSize="sm">
              Run the task to collect history.
            </Text>
          </Box>
        ) : (
          <Table.Root size="sm" striped>
            <Table.Body>
              <Table.Row>
                <Table.ColumnHeader>Started</Table.ColumnHeader>
                <Table.ColumnHeader>Status</Table.ColumnHeader>
                <Table.ColumnHeader>Observed Value</Table.ColumnHeader>
                <Table.ColumnHeader>Condition</Table.ColumnHeader>
                <Table.ColumnHeader>Duration</Table.ColumnHeader>
                <Table.ColumnHeader>Triggered By</Table.ColumnHeader>
                <Table.ColumnHeader>Run</Table.ColumnHeader>
              </Table.Row>
              {history.map((record) => (
                <Table.Row key={`${record.run.run_uid}-${record.rule_uid}`}>
                  <Table.Cell>{formatDate(record.run.started_at)}</Table.Cell>
                  <Table.Cell>
                    <RuleStatusBadge status={record.status} />
                  </Table.Cell>
                  <Table.Cell>{record.observed_value ?? "-"}</Table.Cell>
                  <Table.Cell>{formatCondition(record.condition)}</Table.Cell>
                  <Table.Cell>{formatDuration(record.duration_ms)}</Table.Cell>
                  <Table.Cell>{getTriggeredBy(record.run.run_id)}</Table.Cell>
                  <Table.Cell>
                    <Link colorPalette="blue" href={buildTaskInstanceHref(record.run)}>
                      {record.run.run_id}
                    </Link>
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table.Root>
        )}
        {!loading && history.length > 0 ? (
          <Flex justifyContent="center" mt={3}>
            {nextCursor === null ? (
              <Text color="fg.muted" fontSize="xs">
                No more history.
              </Text>
            ) : (
              <Button loading={loadingMore} onClick={onLoadMore} size="sm" variant="outline">
                Load more
              </Button>
            )}
          </Flex>
        ) : null}
      </Box>

      <Box borderRadius="lg" borderWidth="1px" p={4}>
        <Flex alignItems="center" justifyContent="space-between">
          <Text fontSize="md" fontWeight="semibold">
            SQL Executed
          </Text>
          <Button onClick={() => setSqlExpanded((expanded) => !expanded)} size="sm" variant="outline">
            {sqlExpanded ? "Hide" : "Show"}
          </Button>
        </Flex>
        {sqlExpanded ? (
          <Box mt={3}>
            <Flex justifyContent="flex-end" mb={2}>
              <Button onClick={() => void navigator.clipboard.writeText(sql)} size="xs" variant="outline">
                Copy
              </Button>
            </Flex>
            <Code as="pre" display="block" overflowX="auto" p={3} whiteSpace="pre-wrap">
              {sql}
            </Code>
          </Box>
        ) : null}
      </Box>
    </Box>
  );
};
