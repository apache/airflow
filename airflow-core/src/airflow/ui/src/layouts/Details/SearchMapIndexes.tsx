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
import { useLocation, useNavigate } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { UseTaskInstanceServiceGetMappedTaskInstancesKeyFn } from "openapi/queries";
import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";

import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import { buildTaskInstanceUrl } from "src/utils/links";
import type { MapIndexSearchOption } from "src/utils/option";

import { BY_MAP_INDEX, SEARCH_LIMIT, buildMapIndexOption } from "./searchOptions";

const formatOptionLabel = (option: MapIndexSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    <StateBadge flexShrink={0} state={option.state} />
    <Text truncate>{option.label}</Text>
  </Flex>
);

export const SearchMapIndexes = ({
  dagId,
  instances,
  onClose,
  runId,
  taskId,
}: {
  readonly dagId: string;
  /** Already loaded by the breadcrumb level, so opening the panel shows them straight away. */
  readonly instances: Array<MapIndexSearchOption>;
  readonly onClose: () => void;
  readonly runId: string;
  readonly taskId: string;
}) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { pathname } = useLocation();

  const onSelect = (selected: SingleValue<MapIndexSearchOption>) => {
    if (!selected) {
      return;
    }

    onClose();
    void Promise.resolve(
      navigate(
        buildTaskInstanceUrl({
          currentPathname: pathname,
          dagId,
          isMapped: true,
          mapIndex: selected.value,
          runId,
          taskId,
        }),
      ),
    );
  };

  const searchMapIndexesDebounced = useDebouncedCallback(
    (
      inputValue: string,
      callback: (options: OptionsOrGroups<MapIndexSearchOption, GroupBase<MapIndexSearchOption>>) => void,
    ) => {
      void queryClient.fetchQuery({
        queryFn: () =>
          TaskInstanceService.getMappedTaskInstances({
            dagId,
            dagRunId: runId,
            limit: SEARCH_LIMIT,
            orderBy: BY_MAP_INDEX,
            renderedMapIndexPattern: inputValue,
            taskId,
          }).then((matches: TaskInstanceCollectionResponse) => {
            const options = matches.task_instances.map(buildMapIndexOption);

            callback(options);

            return options;
          }),
        queryKey: UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({
          dagId,
          dagRunId: runId,
          limit: SEARCH_LIMIT,
          orderBy: BY_MAP_INDEX,
          renderedMapIndexPattern: inputValue,
          taskId,
        }),
        staleTime: 0,
      });
    },
    300,
  );

  return (
    <SearchSelect
      defaultOptions={instances}
      formatOptionLabel={formatOptionLabel}
      loadingMessage={translate("common:loading")}
      loadOptions={searchMapIndexesDebounced}
      onChange={onSelect}
      placeholder={translate("search.mapIndexes")}
    />
  );
};
