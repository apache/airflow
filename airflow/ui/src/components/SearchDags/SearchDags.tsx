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
import { Field } from "@chakra-ui/react";
import { useQueryClient } from "@tanstack/react-query";
import { AsyncSelect } from "chakra-react-select";
import type { OptionsOrGroups, GroupBase, SingleValue } from "chakra-react-select";
import debounce from "debounce-promise";
import React from "react";
import { useNavigate } from "react-router-dom";

import { UseDagServiceGetDagsKeyFn } from "openapi/queries";
import { DagService } from "openapi/requests/services.gen";
import type { DAGCollectionResponse, DAGResponse } from "openapi/requests/types.gen";
import type { Option } from "src/utils/option";

import { DropdownIndicator } from "./SearchDagsDropdownIndicator";

export const SearchDags = ({
  setIsOpen,
}: {
  readonly setIsOpen: React.Dispatch<React.SetStateAction<boolean>>;
}) => {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const SEARCH_LIMIT = 10;

  const onSelect = (selected: SingleValue<Option>) => {
    if (selected) {
      setIsOpen(false);
      navigate(`/dags/${selected.value}`);
    }
  };

  const searchDag = (
    inputValue: string,
    callback: (options: OptionsOrGroups<Option, GroupBase<Option>>) => void,
  ): Promise<OptionsOrGroups<Option, GroupBase<Option>>> =>
    queryClient.fetchQuery({
      queryFn: () =>
        DagService.getDags({
          dagDisplayNamePattern: inputValue,
          limit: SEARCH_LIMIT,
        }).then((data: DAGCollectionResponse) => {
          const options = data.dags.map((dag: DAGResponse) => ({
            label: dag.dag_display_name || dag.dag_id,
            value: dag.dag_id,
          }));

          callback(options);

          return options;
        }),
      queryKey: UseDagServiceGetDagsKeyFn({
        dagDisplayNamePattern: inputValue,
      }),
      staleTime: 0,
    });

  const searchDagDebounced = debounce(searchDag, 300);

  return (
    <Field.Root>
      <AsyncSelect
        backspaceRemovesValue={true}
        components={{ DropdownIndicator }}
        defaultOptions
        filterOption={undefined}
        loadOptions={searchDagDebounced}
        menuIsOpen
        onChange={onSelect}
        placeholder="Search Dags"
        // eslint-disable-next-line unicorn/no-null
        value={null} // null is required https://github.com/JedWatson/react-select/issues/3066
      />
    </Field.Root>
  );
};
