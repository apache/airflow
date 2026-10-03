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
import { useEffect, useState } from "react";

import { Box, Button, Heading, HStack, Link, Stack, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as RouterLink, useParams, useSearchParams } from "react-router-dom";

import { useDagRunServiceGetDagRun, useDagRunServiceGetExecution } from "openapi/queries";
import type { ExecutionRegionResponse, ExecutionTaskResponse } from "openapi/requests/types.gen";

import { Checkbox, Pagination, ProgressBar } from "src/system-components";

import { ClearExecutionDialog } from "src/components/Clear/TaskInstance/ClearExecutionDialog";
import { ErrorAlert } from "src/components/ErrorAlert";
import { StateBadge } from "src/components/StateBadge";

import { SearchParamsKeys } from "src/constants/searchParams";
import { isStatePending, useAutoRefresh } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

const PAGE_SIZE = 100;

type Group = {
  index?: number;
  nodeId?: string;
  tasks: Array<ExecutionTaskResponse>;
};

const groupExecutions = (tasks: Array<ExecutionTaskResponse>, regions: Array<ExecutionRegionResponse>) => {
  const byRegion = new Map(regions.map((region) => [region.id, region]));
  const groups = new Map<string, Group>();

  for (const task of tasks) {
    const region = byRegion.get(task.region_id);
    const mapped = region?.node_id === task.task_id;
    const parentId = region?.parent_region_id;
    const parent = parentId === undefined || parentId === null ? undefined : byRegion.get(parentId);
    const nodeId = mapped ? parent?.node_id : region?.node_id;
    const index =
      nodeId === undefined
        ? undefined
        : mapped
          ? (region.parent_region_index ?? undefined)
          : task.region_index;
    const key = JSON.stringify([nodeId, index]);
    const group = groups.get(key) ?? { index, nodeId, tasks: [] };

    group.tasks.push(task);
    groups.set(key, group);
  }

  return [...groups.entries()].sort(
    ([, first], [, second]) =>
      (first.nodeId ?? "").localeCompare(second.nodeId ?? "") || (first.index ?? -1) - (second.index ?? -1),
  );
};

const executionLink = (task: ExecutionTaskResponse) => {
  const path = getTaskInstanceLink(
    { dagId: task.dag_id, dagRunId: task.dag_run_id, mapIndex: task.map_index, taskId: task.task_id },
    "logs",
  );
  const query = new URLSearchParams({
    region_id: task.region_id,
    region_index: String(task.region_index),
    try_number: String(task.try_number),
  });

  return `${path}?${query}`;
};

const TaskRow = ({
  onSelect,
  selected,
  selectLabel,
  task,
}: {
  readonly onSelect: () => void;
  readonly selected: boolean;
  readonly selectLabel: string;
  readonly task: ExecutionTaskResponse;
}) => {
  const { t: translate } = useTranslation();

  return (
    <HStack justify="space-between" py={1}>
      <Checkbox aria-label={selectLabel} checked={selected} onCheckedChange={onSelect} />
      <Link asChild>
        <RouterLink to={executionLink(task)}>
          {task.task_display_name}
          {task.map_index >= 0 ? ` [${task.map_index}]` : ""}
        </RouterLink>
      </Link>
      <StateBadge state={task.state}>{translate(`common:states.${task.state ?? "none"}`)}</StateBadge>
    </HStack>
  );
};

const ExecutionView = () => {
  const { dagId = "", runId = "" } = useParams();
  const { t: translate } = useTranslation("dag");
  const [searchParams, setSearchParams] = useSearchParams();
  const parsedOffset = Number(searchParams.get(SearchParamsKeys.EXECUTION_OFFSET) ?? 0);
  const offset = Number.isInteger(parsedOffset) && parsedOffset >= 0 ? parsedOffset : 0;
  const refresh = useAutoRefresh({ dagId });
  const { data: dagRun } = useDagRunServiceGetDagRun({ dagId, dagRunId: runId }, undefined, {
    refetchInterval: (query) => isStatePending(query.state.data?.state) && refresh,
  });
  const { data, error, isLoading } = useDagRunServiceGetExecution(
    { dagId, dagRunId: runId, limit: PAGE_SIZE, offset },
    undefined,
    { refetchInterval: isStatePending(dagRun?.state) && refresh },
  );
  const totalEntries = data?.total_entries ?? 0;

  useEffect(() => {
    if (data !== undefined && offset > 0 && offset >= totalEntries) {
      setSearchParams(
        (previous) => {
          const updated = new URLSearchParams(previous);
          const lastOffset = Math.max(0, Math.ceil(totalEntries / PAGE_SIZE) - 1) * PAGE_SIZE;

          if (lastOffset === 0) {
            updated.delete(SearchParamsKeys.EXECUTION_OFFSET);
          } else {
            updated.set(SearchParamsKeys.EXECUTION_OFFSET, String(lastOffset));
          }

          return updated;
        },
        { replace: true },
      );
    }
  }, [data, offset, setSearchParams, totalEntries]);
  const [expanded, setExpanded] = useState(new Set<string>());
  const [selected, setSelected] = useState(new Map<string, ExecutionTaskResponse>());
  const [clearing, setClearing] = useState(false);
  const row = (task: ExecutionTaskResponse) => (
    <TaskRow
      key={task.id}
      onSelect={() =>
        setSelected((previous) => {
          const updated = new Map(previous);

          if (updated.has(task.id)) {
            updated.delete(task.id);
          } else {
            updated.set(task.id, task);
          }

          return updated;
        })
      }
      selected={selected.has(task.id)}
      selectLabel={translate("execution.select", { task: task.task_display_name })}
      task={task}
    />
  );
  const groups = groupExecutions(data?.task_instances ?? [], data?.regions ?? []);
  const page = (next: number) =>
    setSearchParams((previous) => {
      const updated = new URLSearchParams(previous);

      updated.set(SearchParamsKeys.EXECUTION_OFFSET, String((next - 1) * PAGE_SIZE));

      return updated;
    });

  return (
    <Stack gap={4} p={4}>
      <Heading size="lg">{translate("execution.title")}</Heading>
      <Button alignSelf="start" disabled={selected.size === 0} onClick={() => setClearing(true)}>
        {translate("execution.clearSelected")}
      </Button>
      {clearing ? (
        <ClearExecutionDialog
          dagId={dagId}
          executions={[...selected.values()]}
          onClose={() => {
            setClearing(false);
            setSelected(new Map());
          }}
          open
          runId={runId}
        />
      ) : undefined}
      <ErrorAlert error={error} />
      {isLoading ? <ProgressBar size="xs" /> : undefined}
      {data !== undefined && totalEntries === 0 ? <Text>{translate("execution.empty")}</Text> : undefined}
      {totalEntries > 0 ? (
        <Text>
          {translate("execution.page", {
            end: Math.min(offset + (data?.task_instances.length ?? 0), totalEntries),
            start: (data?.task_instances.length ?? 0) > 0 ? offset + 1 : 0,
            total: totalEntries,
          })}
        </Text>
      ) : undefined}
      {groups.map(([key, group]) => {
        const tasksById = new Map<string, Array<ExecutionTaskResponse>>();

        for (const task of group.tasks) {
          const tasks = tasksById.get(task.task_id) ?? [];

          tasks.push(task);
          tasksById.set(task.task_id, tasks);
        }

        return (
          <Box borderRadius="md" borderWidth="1px" key={key} p={3}>
            <Heading size="md">
              {group.nodeId === undefined
                ? translate("execution.runTasks")
                : translate("execution.iteration", { index: group.index, node: group.nodeId })}
            </Heading>
            {[...tasksById].map(([taskId, tasks]) => {
              const mapped = tasks.every((task) => task.map_index >= 0);
              const expansionKey = `${key}:${taskId}`;

              return mapped ? (
                <Box key={taskId}>
                  <Button
                    aria-expanded={expanded.has(expansionKey)}
                    onClick={() =>
                      setExpanded((previous) => {
                        const updated = new Set(previous);

                        if (updated.has(expansionKey)) {
                          updated.delete(expansionKey);
                        } else {
                          updated.add(expansionKey);
                        }

                        return updated;
                      })
                    }
                    variant="ghost"
                  >
                    {translate("execution.mapped", {
                      count: tasks.length,
                      task: tasks[0]?.task_display_name,
                    })}
                  </Button>
                  {expanded.has(expansionKey) ? tasks.map(row) : undefined}
                </Box>
              ) : (
                tasks.map(row)
              );
            })}
          </Box>
        );
      })}
      {totalEntries > PAGE_SIZE ? (
        <Pagination.Root
          count={totalEntries}
          onPageChange={(event) => page(event.page)}
          page={Math.floor(offset / PAGE_SIZE) + 1}
          pageSize={PAGE_SIZE}
        >
          <HStack justify="center">
            <Pagination.PrevTrigger />
            <Pagination.Items />
            <Pagination.NextTrigger />
          </HStack>
        </Pagination.Root>
      ) : undefined}
    </Stack>
  );
};

export const Execution = () => {
  const { dagId, runId } = useParams();

  return <ExecutionView key={JSON.stringify([dagId, runId])} />;
};
