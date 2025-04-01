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
import type { OptionsOrGroups, GroupBase, SingleValue } from "chakra-react-select";
import { AsyncSelect } from "chakra-react-select";
import { useCallback } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { UseDagVersionServiceGetDagVersionsKeyFn } from "openapi/queries";
import { DagVersionService } from "openapi/requests/services.gen";
import type { DAGVersionCollectionResponse, DagVersionResponse } from "openapi/requests/types.gen";
import { SearchParamsKeys } from "src/constants/searchParams";
import useSelectedVersion from "src/hooks/useSelectedVersion";
import type { Option } from "src/utils/option";

const DagVersionSelect = ({
  disabled = false,
  showLabel = true,
}: {
  readonly disabled?: boolean;
  readonly showLabel?: boolean;
}) => {
  const queryClient = useQueryClient();

  const [searchParams, setSearchParams] = useSearchParams();

  const selectedVersion = useSelectedVersion();

  const { dagId = "" } = useParams();

  const loadVersions = (
    _: string,
    callback: (options: OptionsOrGroups<Option, GroupBase<Option>>) => void,
  ): Promise<OptionsOrGroups<Option, GroupBase<Option>>> =>
    queryClient.fetchQuery({
      queryFn: () =>
        DagVersionService.getDagVersions({
          dagId,
        }).then((data: DAGVersionCollectionResponse) => {
          const options = [...data.dag_versions].reverse().map((version: DagVersionResponse) => {
            const versionNumber = version.version_number.toString();

            return {
              label: `v${versionNumber}`,
              value: versionNumber,
            };
          });

          callback(options);

          return options;
        }),
      queryKey: UseDagVersionServiceGetDagVersionsKeyFn({ dagId }),
      staleTime: 0,
    });

  const handleStateChange = useCallback(
    (version: SingleValue<Option>) => {
      if (version) {
        searchParams.set(SearchParamsKeys.VERSION_NUMBER, version.value);
        setSearchParams(searchParams);
      }
    },
    [searchParams, setSearchParams],
  );

  return (
    <Field.Root disabled={disabled} width="fit-content">
      {showLabel ? <Field.Label fontSize="xs">Dag Version</Field.Label> : undefined}
      <AsyncSelect
        defaultOptions
        filterOption={undefined}
        isSearchable={false}
        loadOptions={loadVersions}
        onChange={handleStateChange}
        placeholder="Dag Version"
        size="sm"
        value={
          selectedVersion === undefined
            ? undefined
            : { label: `v${selectedVersion}`, value: selectedVersion.toString() }
        }
      />
    </Field.Root>
  );
};

export default DagVersionSelect;
