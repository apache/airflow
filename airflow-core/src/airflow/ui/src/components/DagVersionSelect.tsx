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
import { useCallback, useMemo, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import { 
  useDagVersionServiceGetDagVersions,
  useDagRunServiceGetDagRun
} from "openapi/queries";
import type { DagVersionResponse } from "openapi/requests/types.gen";
import { SearchParamsKeys } from "src/constants/searchParams";
import useSelectedVersion from "src/hooks/useSelectedVersion";

import Time from "./Time";

type VersionSelected = {
  value: number;
  version: DagVersionResponse;
};

export const DagVersionSelect = ({ showLabel = true }: { readonly showLabel?: boolean }) => {
  const { t: translate } = useTranslation("components");
  const { dagId = "", runId } = useParams();
  const { data, isLoading } = useDagVersionServiceGetDagVersions({ dagId, orderBy: "-version_number" });
  
  const { data: dagRunData } = useDagRunServiceGetDagRun(
    {
      dagId,
      dagRunId: runId ?? "",
    },
    undefined,
    { enabled: Boolean(dagId) && Boolean(runId) },
  );
  
  const [searchParams, setSearchParams] = useSearchParams();
  const selectedVersionNumber = useSelectedVersion();
  
  const availableVersions = useMemo(() => {
    if (!data?.dag_versions) return [];
    
    if (dagRunData?.dag_versions) {
      const dagRunVersionNumbers = new Set(
        dagRunData.dag_versions.map((v: DagVersionResponse) => v.version_number)
      );
      return data.dag_versions.filter((v: DagVersionResponse) => 
        dagRunVersionNumbers.has(v.version_number)
      );
    }
    return data.dag_versions;
  }, [data?.dag_versions, dagRunData?.dag_versions]);
  
  const selectedVersion = availableVersions.find((dv: DagVersionResponse) => dv.version_number === selectedVersionNumber);
  
  useEffect(() => {
    if (selectedVersionNumber && dagRunData?.dag_versions && availableVersions.length > 0) {
      const isVersionAvailable = availableVersions.some((v: DagVersionResponse) => v.version_number === selectedVersionNumber);
      if (!isVersionAvailable) {
        searchParams.delete(SearchParamsKeys.VERSION_NUMBER);
        setSearchParams(searchParams);
      }
    }
  }, [selectedVersionNumber, dagRunData?.dag_versions, availableVersions, searchParams, setSearchParams]);
  
  const versionOptions = useMemo(
    () =>
      createListCollection({
        items: availableVersions.map((dv: DagVersionResponse) => ({ value: dv.version_number, version: dv })),
      }),
    [availableVersions],
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
      disabled={isLoading || availableVersions.length === 0}
      onValueChange={handleStateChange}
      size="sm"
      value={selectedVersionNumber === undefined ? [] : [selectedVersionNumber.toString()]}
      width="250px"
    >
      {showLabel ? (
        <Select.Label fontSize="xs">{translate("versionSelect.dagVersion")}</Select.Label>
      ) : undefined}
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText placeholder="All Versions">
            {selectedVersion === undefined ? undefined : (
              <Flex justifyContent="space-between" width="175px">
                <Text>
                  {translate("versionSelect.versionCode", { versionCode: selectedVersion.version_number })}
                </Text>
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
          {versionOptions.items.map((option: VersionSelected) => (
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
