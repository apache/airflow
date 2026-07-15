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

import { Box, Button, Flex, Input, Link, Spinner, Table, Text } from "@chakra-ui/react";
import { type FC, useMemo, useState } from "react";

import { DQErrorState } from "src/components/DQErrorState";
import { DQSummaryCards } from "src/components/DQSummaryCards";
import { NoDQResult } from "src/components/NoDQResult";
import { RuleHistoryPage } from "src/components/RuleHistoryPage";
import { RuleStatusBadge } from "src/components/RuleStatusBadge";
import { useTaskDQ } from "src/hooks/useTaskDQ";
import type { RuleResultRecord, TaskDQRunRecord } from "src/types/dq";
import { buildTaskInstanceHref } from "src/utils/taskInstanceLink";

interface TaskDQOverviewProps {
  dagId: string;
  taskId: string;
}

interface RuleOverview {
  errored: number;
  failures: number;
  latest: RuleResultRecord;
  latestStartedAt: string | null;
  passes: number;
  runs: number;
  warnings: number;
}

type RuleFilter = "all" | "error" | "fail" | "issues" | "pass" | "warn";

const RULE_FILTERS: Array<{ label: string; value: RuleFilter }> = [
  { label: "All", value: "all" },
  { label: "Issues", value: "issues" },
  { label: "Pass", value: "pass" },
  { label: "Warn", value: "warn" },
  { label: "Fail", value: "fail" },
  { label: "Error", value: "error" },
];

function formatDate(value: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.valueOf())) return value;
  return date.toLocaleString();
}

function getRuleOverviews(runs: Array<TaskDQRunRecord>): Array<RuleOverview> {
  const rules = new Map<string, RuleOverview>();

  for (const run of runs) {
    for (const result of run.results) {
      const current = rules.get(result.rule_uid);
      const errored = result.status === "error" ? 1 : 0;
      const failures = result.status === "fail" || result.status === "error" ? 1 : 0;
      const passes = result.status === "pass" ? 1 : 0;
      const warnings = result.status === "warn" ? 1 : 0;

      if (current === undefined) {
        rules.set(result.rule_uid, {
          errored,
          failures,
          latest: result,
          latestStartedAt: run.run.started_at,
          passes,
          runs: 1,
          warnings,
        });
      } else {
        rules.set(result.rule_uid, {
          errored: current.errored + errored,
          failures: current.failures + failures,
          latest: current.latest,
          latestStartedAt: current.latestStartedAt,
          passes: current.passes + passes,
          runs: current.runs + 1,
          warnings: current.warnings + warnings,
        });
      }
    }
  }

  return [...rules.values()].sort((left, right) => left.latest.rule_name.localeCompare(right.latest.rule_name));
}

function getLatestRun(runs: Array<TaskDQRunRecord>): TaskDQRunRecord | undefined {
  return runs[0];
}

export const TaskDQOverview: FC<TaskDQOverviewProps> = ({ dagId, taskId }) => {
  const [ruleFilter, setRuleFilter] = useState<RuleFilter>("all");
  const [refreshKey, setRefreshKey] = useState(0);
  const [ruleQuery, setRuleQuery] = useState("");
  const [historyRuleUid, setHistoryRuleUid] = useState<string | null>(null);
  const {
    error,
    loading,
    loadMoreRuleHistory,
    loadMoreRuns,
    notFound,
    ruleHistory,
    ruleHistoryError,
    ruleHistoryLoading,
    ruleHistoryLoadingMore,
    ruleHistoryNextCursor,
    runs,
    runsLoadingMore,
    runsNextCursor,
  } = useTaskDQ(dagId, taskId, historyRuleUid, refreshKey);
  const ruleOverviews = useMemo(() => getRuleOverviews(runs), [runs]);
  const filteredRuleOverviews = useMemo(
    () =>
      ruleOverviews.filter((rule) => {
        const matchesQuery = rule.latest.rule_name.toLowerCase().includes(ruleQuery.trim().toLowerCase());
        const matchesFilter =
          ruleFilter === "all" ||
          (ruleFilter === "issues" && (rule.failures > 0 || rule.warnings > 0)) ||
          (ruleFilter === "pass" && rule.passes > 0) ||
          (ruleFilter === "warn" && rule.warnings > 0) ||
          (ruleFilter === "fail" && rule.failures > rule.errored) ||
          (ruleFilter === "error" && rule.errored > 0);

        return matchesQuery && matchesFilter;
      }),
    [ruleFilter, ruleOverviews, ruleQuery],
  );
  const latestRun = getLatestRun(runs);
  const historyRule = ruleOverviews.find((rule) => rule.latest.rule_uid === historyRuleUid);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" p={8}>
        <Spinner size="md" />
      </Box>
    );
  }

  if (error) {
    return <DQErrorState message={error} />;
  }

  if (notFound || runs.length === 0 || latestRun === undefined) {
    return <NoDQResult />;
  }

  if (historyRule) {
    return (
      <RuleHistoryPage
        history={ruleHistory}
        loading={ruleHistoryLoading}
        loadingMore={ruleHistoryLoadingMore}
        nextCursor={ruleHistoryNextCursor}
        onBack={() => setHistoryRuleUid(null)}
        onLoadMore={loadMoreRuleHistory}
        onRefresh={() => setRefreshKey((key) => key + 1)}
        rule={historyRule.latest}
        ruleHistoryError={ruleHistoryError}
      />
    );
  }

  return (
    <Box p={4}>
      <Box borderRadius="lg" borderWidth="1px" p={4}>
        <Flex alignItems="center" gap={3} justifyContent="space-between" mb={4} wrap="wrap">
          <Text fontSize="md" fontWeight="semibold">
            Data Quality Results
          </Text>
          <Button onClick={() => setRefreshKey((key) => key + 1)} size="sm" variant="outline">
            Refresh
          </Button>
        </Flex>

        <DQSummaryCards summary={latestRun.summary} />

        <Flex alignItems="center" gap={3} mb={3} wrap="wrap">
          <Input
            maxW="280px"
            onChange={(event) => setRuleQuery(event.target.value)}
            placeholder="Filter rules"
            size="sm"
            value={ruleQuery}
          />
          <Flex gap={1} wrap="wrap">
            {RULE_FILTERS.map((filter) => (
              <Button
                key={filter.value}
                onClick={() => setRuleFilter(filter.value)}
                size="xs"
                variant={ruleFilter === filter.value ? "solid" : "outline"}
              >
                {filter.label}
              </Button>
            ))}
          </Flex>
        </Flex>

        <Table.Root size="sm" striped>
          <Table.Body>
            <Table.Row>
              <Table.ColumnHeader>Rule</Table.ColumnHeader>
              <Table.ColumnHeader>Latest</Table.ColumnHeader>
              <Table.ColumnHeader>Runs</Table.ColumnHeader>
              <Table.ColumnHeader>Failures</Table.ColumnHeader>
              <Table.ColumnHeader>Warnings</Table.ColumnHeader>
              <Table.ColumnHeader>Last Value</Table.ColumnHeader>
              <Table.ColumnHeader>Last Run</Table.ColumnHeader>
              <Table.ColumnHeader>History</Table.ColumnHeader>
            </Table.Row>
            {filteredRuleOverviews.map((rule) => (
              <Table.Row key={rule.latest.rule_uid}>
                <Table.Cell>
                  <Text fontWeight="medium">{rule.latest.rule_name}</Text>
                  {rule.latest.description ? (
                    <Text color="fg.muted" fontSize="xs">
                      {rule.latest.description}
                    </Text>
                  ) : null}
                </Table.Cell>
                <Table.Cell>
                  <RuleStatusBadge status={rule.latest.status} />
                </Table.Cell>
                <Table.Cell>{rule.runs}</Table.Cell>
                <Table.Cell>{rule.failures}</Table.Cell>
                <Table.Cell>{rule.warnings}</Table.Cell>
                <Table.Cell>{rule.latest.observed_value ?? "-"}</Table.Cell>
                <Table.Cell>{formatDate(rule.latestStartedAt)}</Table.Cell>
                <Table.Cell>
                  <Button onClick={() => setHistoryRuleUid(rule.latest.rule_uid)} size="xs" variant="outline">
                    View History
                  </Button>
                </Table.Cell>
              </Table.Row>
            ))}
            {filteredRuleOverviews.length === 0 ? (
              <Table.Row>
                <Table.Cell colSpan={8}>
                  <Text color="fg.muted" fontSize="sm">
                    No rules match the current filters.
                  </Text>
                </Table.Cell>
              </Table.Row>
            ) : null}
          </Table.Body>
        </Table.Root>
      </Box>

      <Box borderRadius="lg" borderWidth="1px" mt={4} p={4}>
        <Text fontSize="md" fontWeight="semibold" mb={3}>
          Recent Runs
        </Text>
        <Table.Root size="sm" striped>
          <Table.Body>
            <Table.Row>
              <Table.ColumnHeader>Started</Table.ColumnHeader>
              <Table.ColumnHeader>Score</Table.ColumnHeader>
              <Table.ColumnHeader>Passed</Table.ColumnHeader>
              <Table.ColumnHeader>Warned</Table.ColumnHeader>
              <Table.ColumnHeader>Failed</Table.ColumnHeader>
              <Table.ColumnHeader>Errored</Table.ColumnHeader>
              <Table.ColumnHeader />
            </Table.Row>
            {runs.map((run) => (
              <Table.Row key={run.run.run_uid}>
                <Table.Cell>{formatDate(run.run.started_at)}</Table.Cell>
                <Table.Cell>{run.summary.score === null ? "-" : run.summary.score.toFixed(2)}</Table.Cell>
                <Table.Cell>{run.summary.passed}</Table.Cell>
                <Table.Cell>{run.summary.warned}</Table.Cell>
                <Table.Cell>{run.summary.failed}</Table.Cell>
                <Table.Cell>{run.summary.errored}</Table.Cell>
                <Table.Cell>
                  <Link colorPalette="blue" href={buildTaskInstanceHref(run.run)}>
                    View run
                  </Link>
                </Table.Cell>
              </Table.Row>
            ))}
          </Table.Body>
        </Table.Root>
        <Flex justifyContent="center" mt={3}>
          {runsNextCursor === null ? (
            <Text color="fg.muted" fontSize="xs">
              No more runs.
            </Text>
          ) : (
            <Button loading={runsLoadingMore} onClick={loadMoreRuns} size="sm" variant="outline">
              Load more
            </Button>
          )}
        </Flex>
      </Box>
    </Box>
  );
};
