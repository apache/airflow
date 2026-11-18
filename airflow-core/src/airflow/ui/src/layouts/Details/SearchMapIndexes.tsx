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
import { useQueryClient } from "@tanstack/react-query";
import type { SingleValue } from "chakra-react-select";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { UseTaskInstanceServiceGetMappedTaskInstancesKeyFn } from "openapi/queries";
import { TaskInstanceService } from "openapi/requests/services.gen";
import type { TaskInstanceCollectionResponse } from "openapi/requests/types.gen";

import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import { SearchParamsKeys } from "src/constants/searchParams";
import { buildTaskInstanceUrl } from "src/utils/links";
import type { MapIndexSearchOption } from "src/utils/option";

import { BY_MAP_INDEX, SEARCH_LIMIT, buildMapIndexOptions } from "./searchOptions";

/** Stands for the whole expansion rather than one of them, so it is not a map index. */
const ALL_MAP_INDEXES = "all";

const formatOptionLabel = (option: MapIndexSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    {option.state === undefined ? undefined : <StateBadge flexShrink={0} state={option.state} />}
    <Text truncate>{option.label}</Text>
  </Flex>
);

export const SearchMapIndexes = ({
  currentRegionId,
  dagId,
  instances,
  onClose,
  runId,
  taskId,
}: {
  /** The pass on screen, used to pick which row to keep for an index that spans several. */
  readonly currentRegionId: string | undefined;
  readonly dagId: string;
  /** Already loaded by the breadcrumb level, so opening the panel shows them straight away. */
  readonly instances: Array<MapIndexSearchOption>;
  readonly onClose: () => void;
  readonly runId: string;
  readonly taskId: string;
}) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const [text, setText] = useState("");
  const [found, setFound] = useState<Array<MapIndexSearchOption>>();
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const { pathname } = useLocation();
  const [searchParams] = useSearchParams();

  const onSelect = (selected: SingleValue<MapIndexSearchOption>) => {
    if (!selected) {
      return;
    }

    if (selected.value === ALL_MAP_INDEXES) {
      // The pass stays; only the instance within it is being let go of.
      const params = new URLSearchParams(searchParams);

      params.delete(SearchParamsKeys.REGION_INDEX);
      const query = params.toString();

      onClose();
      void Promise.resolve(
        navigate(`/dags/${dagId}/runs/${runId}/tasks/${taskId}/mapped${query === "" ? "" : `?${query}`}`),
      );

      return;
    }

    const path = buildTaskInstanceUrl({
      currentPathname: pathname,
      dagId,
      isMapped: true,
      mapIndex: selected.value,
      runId,
      taskId,
    });
    // Inside a loop the same map index exists once per iteration, so the path alone does not say
    // which instance was picked.
    const coordinate =
      selected.regionId === undefined
        ? ""
        : `?${new URLSearchParams({ region_id: selected.regionId, region_index: String(selected.regionIndex) })}`;

    onClose();
    void Promise.resolve(navigate(`${path}${coordinate}`));
  };

  const search = useDebouncedCallback((value: string) => {
    void queryClient
      .fetchQuery({
        queryFn: () =>
          TaskInstanceService.getMappedTaskInstances({
            dagId,
            dagRunId: runId,
            limit: SEARCH_LIMIT,
            orderBy: BY_MAP_INDEX,
            renderedMapIndexPattern: value,
            taskId,
          }),
        queryKey: UseTaskInstanceServiceGetMappedTaskInstancesKeyFn({
          dagId,
          dagRunId: runId,
          limit: SEARCH_LIMIT,
          orderBy: BY_MAP_INDEX,
          renderedMapIndexPattern: value,
          taskId,
        }),
        staleTime: 0,
      })
      .then((matched: TaskInstanceCollectionResponse) =>
        setFound(buildMapIndexOptions(matched.task_instances, currentRegionId)),
      );
  }, 300);

  const onInputChange = (value: string) => {
    setText(value);

    if (value === "") {
      search.cancel();
      setFound(undefined);
    } else {
      search(value);
    }
  };

  return (
    <SearchSelect
      formatOptionLabel={formatOptionLabel}
      inputValue={text}
      onChange={onSelect}
      onInputChange={onInputChange}
      // Only offered when nothing is typed: it is the way back out, not a search result.
      options={
        text === ""
          ? [{ label: translate("dag:allMapIndexes"), value: ALL_MAP_INDEXES }, ...instances]
          : (found ?? instances)
      }
      placeholder={translate("search.mapIndexes")}
    />
  );
};
