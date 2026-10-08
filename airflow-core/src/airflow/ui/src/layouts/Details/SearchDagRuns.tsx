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
import { useLocation, useNavigate, useParams } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { UseDagRunServiceGetDagRunsKeyFn } from "openapi/queries";
import { DagRunService } from "openapi/requests/services.gen";
import type { DAGRunCollectionResponse } from "openapi/requests/types.gen";

import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import { buildTaskInstanceUrl } from "src/utils/links";
import type { DagRunSearchOption } from "src/utils/option";

import { NEWEST_FIRST, SEARCH_LIMIT, buildDagRunOption } from "./searchOptions";

const formatOptionLabel = (option: DagRunSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    <StateBadge flexShrink={0} state={option.state} />
    <Text truncate>{option.label}</Text>
  </Flex>
);

export const SearchDagRuns = ({
  dagId,
  isMapped,
  onClose,
  runs,
}: {
  readonly dagId: string;
  /** Whether the task in view has expanded instances, so the switch lands on its list, not a row. */
  readonly isMapped: boolean;
  readonly onClose: () => void;
  /** Already loaded by the breadcrumb level, so opening the panel shows them straight away. */
  readonly runs: Array<DagRunSearchOption>;
}) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { groupId, taskId } = useParams();
  const { pathname } = useLocation();

  // The task in view carries across the switch, through the same builder the grid and keyboard
  // navigation use: it keeps the open tab, and sends an expanded task to its list of instances
  // rather than to a map index that the run picked is not guaranteed to have.
  const onSelect = (selected: SingleValue<DagRunSearchOption>) => {
    if (!selected) {
      return;
    }

    const target =
      groupId === undefined && taskId === undefined
        ? `/dags/${dagId}/runs/${selected.value}`
        : buildTaskInstanceUrl({
            currentPathname: pathname,
            dagId,
            isGroup: groupId !== undefined,
            isMapped,
            runId: selected.value,
            taskId: groupId ?? taskId ?? "",
          });

    onClose();
    void Promise.resolve(navigate(target));
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
            limit: SEARCH_LIMIT,
            orderBy: NEWEST_FIRST,
            runIdPattern: inputValue,
          }).then((matches: DAGRunCollectionResponse) => {
            const options = matches.dag_runs.map(buildDagRunOption);

            callback(options);

            return options;
          }),
        queryKey: UseDagRunServiceGetDagRunsKeyFn({
          dagId,
          limit: SEARCH_LIMIT,
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
      loadingMessage={translate("common:loading")}
      loadOptions={searchDagRunsDebounced}
      onChange={onSelect}
      placeholder={translate("search.dagRuns")}
    />
  );
};
