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
import { createListCollection, Flex, Select, type SelectValueChangeDetails, Text } from "@chakra-ui/react";
import { useCallback, useMemo } from "react";
import { useParams, useSearchParams } from "react-router-dom";

import { useDagVersionServiceGetDagVersions } from "openapi/queries";
import type { DagVersionResponse } from "openapi/requests/types.gen";
import { SearchParamsKeys } from "src/constants/searchParams";
import useSelectedVersion from "src/hooks/useSelectedVersion";

import Time from "./Time";

type VersionSelected = {
  value: number;
  version: DagVersionResponse;
};

export const DagVersionSelect = ({ showLabel = true }: { readonly showLabel?: boolean }) => {
  const { dagId = "" } = useParams();

  const { data, isLoading } = useDagVersionServiceGetDagVersions({ dagId, orderBy: "-version_number" });

  const [searchParams, setSearchParams] = useSearchParams();

  const selectedVersionNumber = useSelectedVersion();

  const selectedVersion = data?.dag_versions.find((dv) => dv.version_number === selectedVersionNumber);

  const versionOptions = useMemo(
    () =>
      createListCollection({
        items: (data?.dag_versions ?? []).map((dv) => ({ value: dv.version_number, version: dv })),
      }),
    [data],
  );

  const handleStateChange = useCallback(
    ({ items }: SelectValueChangeDetails<VersionSelected>) => {
      if (items[0]) {
        searchParams.set(SearchParamsKeys.VERSION_NUMBER, items[0].value.toString());
        setSearchParams(searchParams);
      }
    },
    [searchParams, setSearchParams],
  );

  return (
    <Select.Root
      collection={versionOptions}
      data-testid="dag-run-select"
      disabled={isLoading || !data?.dag_versions}
      onValueChange={handleStateChange}
      size="sm"
      value={selectedVersionNumber === undefined ? [] : [selectedVersionNumber.toString()]}
      width="250px"
    >
      {showLabel ? <Select.Label fontSize="xs">Dag Version</Select.Label> : undefined}
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText placeholder="All Versions">
            {selectedVersion === undefined ? undefined : (
              <Flex justifyContent="space-between" width="175px">
                <Text>v{selectedVersion.version_number}</Text>
                <Time datetime={selectedVersion.created_at} />
              </Flex>
            )}
          </Select.ValueText>
        </Select.Trigger>
        <Select.IndicatorGroup>
          <Select.Indicator />
        </Select.IndicatorGroup>
      </Select.Control>
      <Select.Positioner>
        <Select.Content>
          {versionOptions.items.map((option) => (
            <Select.Item item={option} key={option.version.version_number}>
              <Text>v{option.version.version_number}</Text>
              <Time datetime={option.version.created_at} />
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Positioner>
    </Select.Root>
  );
};
