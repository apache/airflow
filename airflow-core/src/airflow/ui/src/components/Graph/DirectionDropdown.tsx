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
import { createListCollection, Select, type SelectValueChangeDetails } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useLocalStorage } from "usehooks-ts";

import { directionKey } from "src/constants/localStorage";

export type Direction = "DOWN" | "LEFT" | "RIGHT" | "UP";

export const DirectionDropdown = ({ graphId }: { readonly graphId: string }) => {
  const { t: translate } = useTranslation(["components", "dag"]);

  const [direction, setDirection] = useLocalStorage<Direction>(directionKey(graphId), "RIGHT");

  const directionOptions = () =>
    createListCollection({
      items: [
        { label: translate("graph.directionRight"), value: "RIGHT" as Direction },
        { label: translate("graph.directionLeft"), value: "LEFT" as Direction },
        { label: translate("graph.directionUp"), value: "UP" as Direction },
        { label: translate("graph.directionDown"), value: "DOWN" as Direction },
      ],
    });

  const handleDirectionUpdate = (
    event: SelectValueChangeDetails<{ label: string; value: Array<string> }>,
  ) => {
    if (event.value[0] !== undefined) {
      setDirection(event.value[0] as Direction);
    }
  };

  return (
    <Select.Root
      // @ts-expect-error The expected option type is incorrect
      collection={directionOptions()}
      onValueChange={handleDirectionUpdate}
      size="sm"
      value={[direction]}
    >
      <Select.Label fontSize="xs">{translate("dag:panel.graphDirection.label")}</Select.Label>
      <Select.Control>
        <Select.Trigger>
          <Select.ValueText />
        </Select.Trigger>
        <Select.IndicatorGroup>
          <Select.Indicator />
        </Select.IndicatorGroup>
      </Select.Control>
      <Select.Positioner>
        <Select.Content>
          {directionOptions().items.map((option) => (
            <Select.Item item={option} key={option.value}>
              {option.label}
            </Select.Item>
          ))}
        </Select.Content>
      </Select.Positioner>
    </Select.Root>
  );
};
