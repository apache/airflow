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
import { Flex, Text } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import type { GroupBase, OptionsOrGroups, SingleValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";
import { useNavigate, useParams } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { UseDagRunServiceGetDagRunsKeyFn } from "openapi/queries";
import { DagRunService } from "openapi/requests/services.gen";
import type { DAGRunCollectionResponse, DAGRunResponse } from "openapi/requests/types.gen";

import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import type { DagRunSearchOption } from "src/utils/option";

export const DAG_RUN_SEARCH_LIMIT = 10;
export const NEWEST_FIRST = ["-run_after"];

export const buildDagRunOption = (dagRun: DAGRunResponse): DagRunSearchOption => ({
  label: dagRun.dag_run_id,
  state: dagRun.state,
  value: dagRun.dag_run_id,
});

const formatOptionLabel = (option: DagRunSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    <StateBadge flexShrink={0} state={option.state} />
    <Text truncate>{option.label}</Text>
  </Flex>
);

/**
 * Carries the task level across the switch so the same task stays in view in the run picked. A map
 * index is dropped: the same expansion is not guaranteed to exist in another run.
 */
const buildTaskPath = (groupId: string | undefined, taskId: string | undefined) => {
  if (groupId !== undefined) {
    return `/tasks/group/${groupId}`;
  }

  return taskId === undefined ? "" : `/tasks/${taskId}`;
};

export const SearchDagRuns = ({
  dagId,
  onClose,
  runs,
}: {
  readonly dagId: string;
  readonly onClose: () => void;
  /** Already loaded by the breadcrumb level, so opening the panel shows them straight away. */
  readonly runs: Array<DagRunSearchOption>;
}) => {
  const { t: translate } = useTranslation("dags");
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { groupId, taskId } = useParams();

  const onSelect = (selected: SingleValue<DagRunSearchOption>) => {
    if (selected) {
      onClose();
      void Promise.resolve(
        navigate(`/dags/${dagId}/runs/${selected.value}${buildTaskPath(groupId, taskId)}`),
      );
    }
  };

  const searchDagRunsDebounced = useDebouncedCallback(
    (
      inputValue: string,
      callback: (options: OptionsOrGroups<DagRunSearchOption, GroupBase<DagRunSearchOption>>) => void,
    ) => {
      void queryClient.fetchQuery({
        queryFn: () =>
          DagRunService.getDagRuns({
            dagId,
            limit: DAG_RUN_SEARCH_LIMIT,
            orderBy: NEWEST_FIRST,
            runIdPattern: inputValue,
          }).then((matches: DAGRunCollectionResponse) => {
            const options = matches.dag_runs.map(buildDagRunOption);

            callback(options);

            return options;
          }),
        queryKey: UseDagRunServiceGetDagRunsKeyFn({
          dagId,
          limit: DAG_RUN_SEARCH_LIMIT,
          orderBy: NEWEST_FIRST,
          runIdPattern: inputValue,
        }),
        staleTime: 0,
      });
    },
    300,
  );

  return (
    <SearchSelect
      defaultOptions={runs}
      formatOptionLabel={formatOptionLabel}
      loadOptions={searchDagRunsDebounced}
      onChange={onSelect}
      placeholder={translate("search.dagRuns")}
    />
  );
};
