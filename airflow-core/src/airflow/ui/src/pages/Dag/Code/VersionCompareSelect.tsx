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
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useDagVersionServiceGetDagVersions } from "openapi/queries";
import type { DagVersionResponse } from "openapi/requests/types.gen";
import Time from "src/components/Time";

type VersionSelected = {
  value: number;
  version: DagVersionResponse;
};

type VersionCompareSelectProps = {
  readonly excludeVersionNumber?: number;
  readonly label: string;
  readonly onVersionChange: (versionNumber: number) => void;
  readonly placeholder?: string;
  readonly selectedVersionNumber?: number;
};

export const VersionCompareSelect = ({
  excludeVersionNumber,
  label,
  onVersionChange,
  placeholder = "Select version",
  selectedVersionNumber,
}: VersionCompareSelectProps) => {
  const { t: translate } = useTranslation("components");
  const { dagId = "" } = useParams();
  const { data, isLoading } = useDagVersionServiceGetDagVersions({ dagId, orderBy: ["-version_number"] });

  const selectedVersion = data?.dag_versions.find((dv) => dv.version_number === selectedVersionNumber);

  const versionOptions = useMemo(() => {
    const filteredVersions = (data?.dag_versions ?? []).filter(
      (dv) => excludeVersionNumber === undefined || dv.version_number !== excludeVersionNumber,
    );

    return createListCollection({
      items: filteredVersions.map((dv) => ({ value: dv.version_number, version: dv })),
    });
  }, [data, excludeVersionNumber]);

  const handleStateChange = useCallback(
    ({ items }: SelectValueChangeDetails<VersionSelected>) => {
      if (items[0]) {
        onVersionChange(items[0].value);
      }
    },
    [onVersionChange],
  );

  return (
    <Select.Root
      collection={versionOptions}
      disabled={isLoading || !data?.dag_versions}
      onValueChange={handleStateChange}
      size="sm"
      value={selectedVersionNumber === undefined ? [] : [selectedVersionNumber.toString()]}
      w={60}
    >
      <Select.Label fontSize="xs">{label}</Select.Label>
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText placeholder={placeholder}>
            {selectedVersion === undefined ? undefined : (
              <Flex justifyContent="space-between">
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
          {versionOptions.items.map((option) => (
            <Select.Item item={option} key={option.version.version_number}>
              <Text>
                {translate("versionSelect.versionCode", { versionCode: option.version.version_number })}
              </Text>
              <Time datetime={option.version.created_at} />
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Positioner>
    </Select.Root>
  );
};
