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
import { useMatches, useNavigate } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { UseDagServiceGetDagsUiKeyFn } from "openapi/queries";
import { DagService } from "openapi/requests/services.gen";
import type {
  DAGWithLatestDagRunsCollectionResponse,
  DAGWithLatestDagRunsResponse,
} from "openapi/requests/types.gen";

import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import { TabEntity } from "src/constants/tab";
import type { DagSearchOption } from "src/utils/option";
import { getTabPath } from "src/utils/tab";

const formatOptionLabel = (option: DagSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    <StateBadge flexShrink={0} state={option.state} />
    <Text truncate>{option.label}</Text>
  </Flex>
);

export const SearchDags = ({ onClose }: { readonly onClose: () => void }) => {
  const { t: translate } = useTranslation("dags");
  const queryClient = useQueryClient();
  const matches = useMatches();
  const navigate = useNavigate();
  const SEARCH_LIMIT = 10;

  const onSelect = (selected: SingleValue<DagSearchOption>) => {
    if (selected) {
      const additionalPath = getTabPath(matches, TabEntity.Dag);
      const targetPath = additionalPath === "/backfills" && !selected.isBackfillable ? "" : additionalPath;

      onClose();
      void Promise.resolve(navigate(`/dags/${selected.value}${targetPath}`));
    }
  };

  const searchDagDebounced = useDebouncedCallback(
    (
      inputValue: string,
      callback: (options: OptionsOrGroups<DagSearchOption, GroupBase<DagSearchOption>>) => void,
    ) => {
      void queryClient.fetchQuery({
        queryFn: () =>
          DagService.getDagsUi({
            dagDisplayNamePrefixPattern: inputValue,
            dagRunsLimit: 1,
            limit: SEARCH_LIMIT,
          }).then((data: DAGWithLatestDagRunsCollectionResponse) => {
            const options = data.dags.map((dag: DAGWithLatestDagRunsResponse) => ({
              isBackfillable: dag.is_backfillable,
              label: dag.dag_display_name || dag.dag_id,
              state: dag.latest_dag_runs[0]?.state ?? null,
              value: dag.dag_id,
            }));

            callback(options);

            return options;
          }),
        queryKey: UseDagServiceGetDagsUiKeyFn({
          dagDisplayNamePrefixPattern: inputValue,
          dagRunsLimit: 1,
        }),
        staleTime: 0,
      });
    },
    300,
  );

  return (
    <SearchSelect
      formatOptionLabel={formatOptionLabel}
      loadOptions={searchDagDebounced}
      onChange={onSelect}
      placeholder={translate("search.dags")}
    />
  );
};
