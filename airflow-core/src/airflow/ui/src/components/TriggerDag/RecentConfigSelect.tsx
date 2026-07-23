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
import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";

import { useDagRunServiceGetDagRuns } from "openapi/queries";
import type { DAGRunResponse } from "openapi/requests/types.gen";

import Time from "../Time";

type RecentConfigOption = {
  run: DAGRunResponse;
  value: string;
};

type RecentConfigSelectProps = {
  readonly dagId: string;
  readonly onSelectConf: (conf: Record<string, unknown>) => void;
  readonly open: boolean;
};

const RecentConfigSelect = ({ dagId, onSelectConf, open }: RecentConfigSelectProps) => {
  const { t: translate } = useTranslation("components");
  const [selectedValue, setSelectedValue] = useState<Array<string>>([]);
  const { data, isLoading } = useDagRunServiceGetDagRuns(
    { dagId, limit: 25, orderBy: ["-run_after"] },
    undefined,
    { enabled: open },
  );

  const options = useMemo(() => {
    const seenConfs = new Set<string>();

    return (data?.dag_runs ?? []).reduce<Array<RecentConfigOption>>((items, run) => {
      const hasConf = run.conf !== null && Object.keys(run.conf).length > 0;
      const confKey = hasConf ? JSON.stringify(run.conf) : undefined;
      const isNewConf = confKey !== undefined && !seenConfs.has(confKey);

      if (isNewConf) {
        seenConfs.add(confKey);
        items.push({ run, value: run.dag_run_id });
      }

      return items;
    }, []);
  }, [data?.dag_runs]);

  const recentConfigOptions = createListCollection({
    items: options,
    itemToString: (item: RecentConfigOption) => item.run.dag_run_id,
  });

  const handleValueChange = ({ items, value }: SelectValueChangeDetails<RecentConfigOption>) => {
    const [selected] = items;

    setSelectedValue(value);
    if (selected?.run.conf) {
      onSelectConf(selected.run.conf);
    }
  };

  if (!isLoading && options.length === 0) {
    return undefined;
  }

  return (
    <Select.Root
      collection={recentConfigOptions}
      data-testid="recent-config-select"
      disabled={isLoading || options.length === 0}
      onValueChange={handleValueChange}
      size="sm"
      value={selectedValue}
    >
      <Select.Label fontSize="xs">{translate("triggerDag.recentConfig")}</Select.Label>
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText placeholder={translate("triggerDag.recentConfigPlaceholder")} />
        </Select.Trigger>
        <Select.IndicatorGroup>
          <Select.Indicator />
        </Select.IndicatorGroup>
      </Select.Control>
      <Select.Positioner>
        <Select.Content maxH="200px" overflowY="auto">
          {recentConfigOptions.items.map((option) => (
            <Select.Item item={option} key={option.run.dag_run_id}>
              <Flex justifyContent="space-between" width="100%">
                <Text>{option.run.dag_run_id}</Text>
                <Time datetime={option.run.run_after} />
              </Flex>
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Positioner>
    </Select.Root>
  );
};

export default RecentConfigSelect;
