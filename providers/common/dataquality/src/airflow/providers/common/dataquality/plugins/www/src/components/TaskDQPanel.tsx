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

import { Box, Flex, Spinner, Text } from "@chakra-ui/react";
import type { FC } from "react";

import { DQErrorState } from "src/components/DQErrorState";
import { DQSummaryCards } from "src/components/DQSummaryCards";
import { NoDQResult } from "src/components/NoDQResult";
import { RuleResultsTable } from "src/components/RuleResultsTable";
import { useTaskInstanceDQ } from "src/hooks/useTaskInstanceDQ";

interface TaskDQPanelProps {
  dagId: string;
  mapIndex: number;
  runId: string;
  taskId: string;
}

const MetadataItem: FC<{ label: string; value: string | null }> = ({ label, value }) =>
  value ? (
    <Text>
      <Text as="span" color="fg.muted">
        {label}:{" "}
      </Text>
      {value}
    </Text>
  ) : null;

export const TaskDQPanel: FC<TaskDQPanelProps> = ({ dagId, mapIndex, runId, taskId }) => {
  const { error, loading, notFound, result } = useTaskInstanceDQ(dagId, taskId, runId, mapIndex);

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

  if (notFound || !result) {
    return <NoDQResult />;
  }

  return (
    <Box p={4}>
      <Box borderRadius="lg" borderWidth="1px" p={4}>
        <Text fontSize="md" fontWeight="semibold" mb={3}>
          Data Quality Results
        </Text>
        <DQSummaryCards summary={result.summary} />
        <Flex fontSize="xs" gap={4} mb={3} wrap="wrap">
          <MetadataItem label="table" value={result.run.table_ref} />
          <MetadataItem label="ruleset" value={result.run.ruleset_name} />
          <MetadataItem label="evaluated" value={result.run.started_at} />
        </Flex>
        <RuleResultsTable results={result.results} />
      </Box>
    </Box>
  );
};
