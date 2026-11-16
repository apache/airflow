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
import { useMemo, useState } from "react";

import { createListCollection, Flex, type SelectValueChangeDetails, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";

import { Select } from "src/system-components";

import type { ParamPresets, ParamsSpec } from "src/queries/useDagParams";

import Time from "../Time";

type ConfigOption = {
  conf: Record<string, unknown>;
  label: string;
  run?: DAGRunResponse;
  value: string;
};

type LoadConfigSelectProps = {
  readonly dagId: string;
  readonly onSelectConf: (conf: Record<string, unknown>) => void;
  readonly open: boolean;
  readonly paramPresets?: ParamPresets;
  readonly paramsDict?: ParamsSpec;
};

/** One drop-down for every conf the trigger form can start from: the Dag author's presets and recent runs. */
const LoadConfigSelect = ({ dagId, onSelectConf, open, paramPresets, paramsDict }: LoadConfigSelectProps) => {
  const { t: translate } = useTranslation("components");
  const [selectedValue, setSelectedValue] = useState<Array<string>>([]);
  const { data, isLoading } = useDagRunServiceGetDagRuns(
    { dagId, limit: 25, orderBy: ["-run_after"] },
    undefined,
    { enabled: open },
  );

  // A preset is applied on top of the Dag defaults, so it always yields the same conf no matter
  // what was edited before it was picked.
  const presetOptions = useMemo(() => {
    const defaults = Object.fromEntries(
      Object.entries(paramsDict ?? {}).map(([key, { value }]) => [key, value]),
    );

    return Object.entries(paramPresets ?? {}).map(([name, values]) => ({
      conf: { ...defaults, ...values },
      label: name,
      value: `preset:${name}`,
    }));
  }, [paramPresets, paramsDict]);

  const recentOptions = useMemo(() => {
    const seenConfs = new Set<string>();

    return (data?.dag_runs ?? []).reduce<Array<ConfigOption>>((items, run) => {
      const hasConf = run.conf !== null && Object.keys(run.conf).length > 0;
      const confKey = hasConf ? JSON.stringify(run.conf) : undefined;
      const isNewConf = confKey !== undefined && !seenConfs.has(confKey);

      if (isNewConf && run.conf) {
        seenConfs.add(confKey);
        items.push({ conf: run.conf, label: run.dag_run_id, run, value: `run:${run.dag_run_id}` });
      }

      return items;
    }, []);
  }, [data?.dag_runs]);

  const configOptions = createListCollection({
    items: [...presetOptions, ...recentOptions],
    itemToString: (item: ConfigOption) => item.label,
  });

  const handleValueChange = ({ items, value }: SelectValueChangeDetails<ConfigOption>) => {
    const [selected] = items;

    setSelectedValue(value);
    if (selected) {
      onSelectConf(selected.conf);
    }
  };

  if (!isLoading && configOptions.items.length === 0) {
    return undefined;
  }

  return (
    <Select.Root
      collection={configOptions}
      data-testid="load-config-select"
      disabled={configOptions.items.length === 0}
      onValueChange={handleValueChange}
      size="sm"
      value={selectedValue}
    >
      <Select.Label fontSize="xs">{translate("triggerDag.selectConfig")}</Select.Label>
      <Select.Trigger>
        <Select.ValueText placeholder={translate("triggerDag.selectConfigPlaceholder")} />
      </Select.Trigger>
      <Select.Content maxH="200px" overflowY="auto">
        {presetOptions.length > 0 ? (
          <Select.ItemGroup label={translate("triggerDag.presets")}>
            {presetOptions.map((option) => (
              <Select.Item item={option} key={option.value}>
                <Text>{option.label}</Text>
              </Select.Item>
            ))}
          </Select.ItemGroup>
        ) : undefined}
        {recentOptions.length > 0 ? (
          <Select.ItemGroup label={translate("triggerDag.recentConfig")}>
            {recentOptions.map((option) => (
              <Select.Item item={option} key={option.value}>
                <Flex justifyContent="space-between" width="100%">
                  <Text>{option.label}</Text>
                  <Time datetime={option.run?.run_after} />
                </Flex>
              </Select.Item>
            ))}
          </Select.ItemGroup>
        ) : undefined}
      </Select.Content>
    </Select.Root>
  );
};

export default LoadConfigSelect;
