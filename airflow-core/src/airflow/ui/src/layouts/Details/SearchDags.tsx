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
import { useMatches, useNavigate } from "react-router-dom";
import { useDebouncedCallback } from "use-debounce";

import { UseDagServiceGetDagsUiKeyFn } from "openapi/queries";
import { DagService } from "openapi/requests/services.gen";
import type { DAGWithLatestDagRunsCollectionResponse } from "openapi/requests/types.gen";

import { AdvancedSearchToggle } from "src/components/AdvancedSearchToggle";
import { SearchSelect } from "src/components/SearchSelect";
import { StateBadge } from "src/components/StateBadge";

import { TabEntity } from "src/constants/tab";
import { useAdvancedSearch } from "src/hooks/useAdvancedSearch";
import type { DagSearchOption } from "src/utils/option";
import { getTabPath } from "src/utils/tab";

import { SEARCH_LIMIT, buildDagOption } from "./searchOptions";

const ADVANCED_SEARCH_KEY = "breadcrumb-dags";

/** Match anywhere in the name, or only at the start — the same choice the Dags list offers. */
const buildNameArg = (value: string, matchAnywhere: boolean) =>
  matchAnywhere ? { dagDisplayNamePattern: value } : { dagDisplayNamePrefixPattern: value };

const formatOptionLabel = (option: DagSearchOption) => (
  <Flex alignItems="center" gap={2} minW={0}>
    <StateBadge flexShrink={0} state={option.state} />
    <Text truncate>{option.label}</Text>
  </Flex>
);

export const SearchDags = ({
  dags,
  onClose,
}: {
  /** Already loaded by the breadcrumb level, so opening the panel shows them straight away. */
  readonly dags: Array<DagSearchOption>;
  readonly onClose: () => void;
}) => {
  const { t: translate } = useTranslation(["dags", "common"]);
  const [text, setText] = useState("");
  const [found, setFound] = useState<Array<DagSearchOption>>();
  // Its own key rather than the Dags list's: toggling here should not quietly change what that
  // page does. A user who wants it everywhere has the global default setting for that.
  const { enabled: matchAnywhere, onToggle } = useAdvancedSearch(ADVANCED_SEARCH_KEY);
  const queryClient = useQueryClient();
  const matches = useMatches();
  const navigate = useNavigate();

  const onSelect = (selected: SingleValue<DagSearchOption>) => {
    if (selected) {
      const additionalPath = getTabPath(matches, TabEntity.Dag);
      const targetPath = additionalPath === "/backfills" && !selected.isBackfillable ? "" : additionalPath;

      onClose();
      void Promise.resolve(navigate(`/dags/${selected.value}${targetPath}`));
    }
  };

  // The mode is an argument rather than read from the closure, so toggling can re-run the search
  // it is already showing without waiting for the state it just set.
  const search = useDebouncedCallback((value: string, anywhere: boolean) => {
    void queryClient
      .fetchQuery({
        queryFn: () =>
          DagService.getDagsUi({
            ...buildNameArg(value, anywhere),
            dagRunsLimit: 1,
            limit: SEARCH_LIMIT,
          }),
        queryKey: UseDagServiceGetDagsUiKeyFn({
          ...buildNameArg(value, anywhere),
          dagRunsLimit: 1,
        }),
        staleTime: 0,
      })
      .then((matched: DAGWithLatestDagRunsCollectionResponse) => setFound(matched.dags.map(buildDagOption)));
  }, 300);

  const onInputChange = (value: string) => {
    setText(value);

    if (value === "") {
      search.cancel();
      setFound(undefined);
    } else {
      search(value, matchAnywhere);
    }
  };

  // Switching how the text is matched is a new search for the same text, not a different text.
  const onToggleMatchAnywhere = (next: boolean) => {
    onToggle(next);

    if (text !== "") {
      search(text, next);
    }
  };

  return (
    <SearchSelect
      controlEnd={
        <AdvancedSearchToggle enabled={matchAnywhere} onToggle={onToggleMatchAnywhere} variant="addon" />
      }
      formatOptionLabel={formatOptionLabel}
      inputValue={text}
      onChange={onSelect}
      onInputChange={onInputChange}
      options={text === "" ? dags : (found ?? dags)}
      placeholder={translate("search.dags")}
    />
  );
};
