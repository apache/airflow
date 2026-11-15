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

import { Flex, Text } from "@chakra-ui/react";
import type { SingleValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";

import { useGridServiceGetDagStructure } from "openapi/queries";
import type { GridNodeResponse, LightGridTaskInstanceSummary } from "openapi/requests/types.gen";

import { AdvancedSearchToggle } from "src/components/AdvancedSearchToggle";
import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import { useAdvancedSearch } from "src/hooks/useAdvancedSearch";
import { useGridTiSummariesStream } from "src/queries/useGridTISummaries.ts";
import { setExecutionCoordinates, stripExecutionParams } from "src/utils/executionParams";
import { buildTaskInstanceUrl } from "src/utils/links";

import { SEARCH_LIMIT } from "./searchOptions";

const ADVANCED_SEARCH_KEY = "breadcrumb-tasks";

type TaskSearchOption = {
  /** The groups the task sits in, which tells apart tasks that share a name in different groups. */
  readonly groupPath: string;
  readonly isMapped: boolean;
  readonly label: string;
  readonly summary?: LightGridTaskInstanceSummary;
  readonly value: string;
};

type TaskNode = Omit<TaskSearchOption, "summary">;

const formatOptionLabel = (option: TaskSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    {option.summary === undefined ? undefined : <StateBadge flexShrink={0} state={option.summary.state} />}
    <Text flexShrink={0} truncate>
      {option.label}
    </Text>
    {option.groupPath === "" ? undefined : (
      <Text color="fg.muted" fontSize="xs" truncate>
        {option.groupPath}
      </Text>
    )}
  </Flex>
);

/**
 * The tasks of the grid's structure, groups flattened away. The structure, not the tasks endpoint,
 * is what says a task inside a mapped group is expanded: the task itself reports `is_mapped: false`.
 */
const flattenTasks = (nodes: Array<GridNodeResponse>, groups: Array<string> = []): Array<TaskNode> =>
  nodes.flatMap((node) =>
    node.children === null || node.children === undefined
      ? [
          {
            groupPath: groups.join(" / "),
            isMapped: Boolean(node.is_mapped),
            label: node.label,
            value: node.id,
          },
        ]
      : flattenTasks(node.children, [...groups, node.label]),
  );

const matchesTask = (option: TaskSearchOption, text: string, matchAnywhere: boolean) => {
  const needle = text.toLowerCase();

  return matchAnywhere
    ? `${option.label} ${option.groupPath}`.toLowerCase().includes(needle)
    : option.label.toLowerCase().startsWith(needle);
};

/** The sub-route of a task page (`events`, `task_instances`, ...), which a switch carries over. */
const getTaskTabPath = (pathname: string, dagId: string, taskId: string) => {
  const base = `/dags/${dagId}/tasks/${taskId}`;

  return pathname.startsWith(`${base}/`) ? pathname.slice(base.length) : "";
};

export const SearchTasks = ({
  currentTaskId,
  dagId,
  onClose,
  runId,
}: {
  readonly currentTaskId: string;
  readonly dagId: string;
  readonly onClose: () => void;
  readonly runId: string | undefined;
}) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const [text, setText] = useState("");
  // Its own key, like the Dag search's, so toggling here leaves other searches as they are.
  const { enabled: matchAnywhere, onToggle } = useAdvancedSearch(ADVANCED_SEARCH_KEY);
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();
  // Loaded when the panel opens rather than with the page, so a page whose switcher is never opened
  // pays for neither, and every opening shows states as they are now.
  const { data: structure } = useGridServiceGetDagStructure({ dagId });
  const { summariesByRunId } = useGridTiSummariesStream({
    dagId,
    runIds: runId === undefined ? [] : [runId],
  });
  const runSummary = runId === undefined ? undefined : summariesByRunId.get(runId);
  // The stream keys by run id alone, so an answer for another Dag's run of the same id is not ours.
  const summaries =
    runSummary?.dag_id === dagId
      ? new Map(runSummary.task_instances.map((summary) => [summary.task_id, summary]))
      : undefined;

  // Within a run, only tasks the run has an instance of: the structure spans several Dag versions,
  // and a task the run never had would open onto a missing instance.
  const options: Array<TaskSearchOption> = flattenTasks(structure ?? []).flatMap((task) => {
    if (summaries === undefined) {
      return [task];
    }
    const summary = summaries.get(task.value);

    return summary === undefined ? [] : [{ ...task, summary }];
  });

  const onSelect = (selected: SingleValue<TaskSearchOption>) => {
    if (!selected) {
      return;
    }

    if (runId === undefined) {
      onClose();
      void Promise.resolve(
        navigate(`/dags/${dagId}/tasks/${selected.value}${getTaskTabPath(pathname, dagId, currentTaskId)}`),
      );

      return;
    }

    const path = buildTaskInstanceUrl({
      currentPathname: pathname,
      dagId,
      isMapped: selected.isMapped,
      runId,
      taskId: selected.value,
    });
    // A task inside a loop has a row per pass and the API needs to be told which; the grid lands on
    // the newest, and so does this.
    const search = setExecutionCoordinates(
      stripExecutionParams(searchParams),
      selected.summary?.latest_region_id,
      selected.summary?.latest_region_index,
    ).toString();

    onClose();
    void Promise.resolve(navigate(`${path}${search === "" ? "" : `?${search}`}`));
  };

  const matched =
    text === "" ? options : options.filter((option) => matchesTask(option, text, matchAnywhere));

  return (
    <SearchSelect
      controlEnd={<AdvancedSearchToggle enabled={matchAnywhere} onToggle={onToggle} variant="addon" />}
      formatOptionLabel={formatOptionLabel}
      inputValue={text}
      onChange={onSelect}
      onInputChange={setText}
      options={matched.slice(0, SEARCH_LIMIT)}
      placeholder={translate("search.tasks")}
    />
  );
};
