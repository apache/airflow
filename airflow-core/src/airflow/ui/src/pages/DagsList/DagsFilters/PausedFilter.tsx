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
import { createListCollection, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";

import { Select } from "src/components/ui";

type Props = {
  readonly defaultShowPaused: string;
  readonly onPausedChange: (details: SelectValueChangeDetails<string>) => void;
  readonly showPaused: string | null;
};

export const PausedFilter = ({ defaultShowPaused, onPausedChange, showPaused }: Props) => {
  const { t: translate } = useTranslation("dags");

  const enabledOptions = createListCollection({
    items: [
      { label: translate("filters.paused.all"), value: "all" },
      { label: translate("filters.paused.active"), value: "false" },
      { label: translate("filters.paused.paused"), value: "true" },
    ],
  });

  return (
    <Select.Root
      collection={enabledOptions}
      onValueChange={onPausedChange}
      value={[showPaused ?? defaultShowPaused]}
    >
      <Select.Trigger colorPalette="blue" isActive={Boolean(showPaused)}>
        <Select.ValueText width={20} />
      </Select.Trigger>
      <Select.Content>
        {enabledOptions.items.map((option) => (
          <Select.Item item={option} key={option.label}>
            {option.label}
          </Select.Item>
        ))}
      </Select.Content>
    </Select.Root>
  );
};
