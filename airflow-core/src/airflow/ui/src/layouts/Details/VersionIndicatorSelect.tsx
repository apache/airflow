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
import { Circle, Flex, Select, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { FiGitCommit } from "react-icons/fi";

import {
  isVersionIndicatorOption,
  showVersionIndicatorOptions,
  VersionIndicatorOptions,
} from "src/constants/showVersionIndicatorOptions";

type VersionIndicatorSelectProps = {
  readonly onChange: (value: VersionIndicatorOptions) => void;
  readonly value: VersionIndicatorOptions;
};

export const VersionIndicatorSelect = ({ onChange, value }: VersionIndicatorSelectProps) => {
  const { t: translate } = useTranslation(["components", "dag"]);

  const handleChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    const [selectedDisplayMode] = event.value;

    if (isVersionIndicatorOption(selectedDisplayMode)) {
      onChange(selectedDisplayMode);
    }
  };

  return (
    <Select.Root
      // @ts-expect-error option type
      collection={showVersionIndicatorOptions}
      onValueChange={handleChange}
      size="sm"
      value={[value]}
    >
      <Select.Label fontSize="xs">{translate("dag:panel.showVersionIndicator.label")}</Select.Label>
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText>
            <Flex alignItems="center" gap={1}>
              {(value === VersionIndicatorOptions.BUNDLE_VERSION ||
                value === VersionIndicatorOptions.ALL) && (
                <FiGitCommit color="var(--chakra-colors-orange-focus-ring)" />
              )}
              {(value === VersionIndicatorOptions.DAG_VERSION || value === VersionIndicatorOptions.ALL) && (
                <Circle bg="orange.focusRing" size="8px" />
              )}
              {translate(showVersionIndicatorOptions.items.find((item) => item.value === value)?.label ?? "")}
            </Flex>
          </Select.ValueText>
        </Select.Trigger>
        <Select.IndicatorGroup>
          <Select.Indicator />
        </Select.IndicatorGroup>
      </Select.Control>
      <Select.Positioner>
        <Select.Content>
          {showVersionIndicatorOptions.items.map((option) => (
            <Select.Item item={option} key={option.value}>
              <Flex alignItems="center" gap={1}>
                {(option.value === VersionIndicatorOptions.BUNDLE_VERSION ||
                  option.value === VersionIndicatorOptions.ALL) && (
                  <FiGitCommit color="var(--chakra-colors-orange-focus-ring)" />
                )}
                {(option.value === VersionIndicatorOptions.DAG_VERSION ||
                  option.value === VersionIndicatorOptions.ALL) && (
                  <Circle bg="orange.focusRing" size="8px" />
                )}
                {translate(option.label)}
              </Flex>
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Positioner>
    </Select.Root>
  );
};
