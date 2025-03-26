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
import {
  HStack,
  IconButton,
  ButtonGroup,
  type StackProps,
  Stack,
  createListCollection,
  type SelectValueChangeDetails,
} from "@chakra-ui/react";
import { FiGrid } from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import DagVersionSelect from "src/components/DagVersionSelect";
import { directionOptions, type Direction } from "src/components/Graph/useGraphLayout";
import { Select } from "src/components/ui";

import { DagRunSelect } from "./DagRunSelect";

type Props = {
  readonly dagView: string;
  readonly limit: number;
  readonly setDagView: (x: "graph" | "grid") => void;
  readonly setLimit: (limit: number) => void;
} & StackProps;

const options = createListCollection({
  items: [
    { label: "Only tasks", value: "tasks" },
    { label: "External conditions", value: "immediate" },
    { label: "All Dag Dependencies", value: "all" },
  ],
});

const deps = ["all", "immediate", "tasks"];

type Dependency = (typeof deps)[number];

export const PanelButtons = ({ dagView, limit, setDagView, setLimit, ...rest }: Props) => {
  const { dagId = "" } = useParams();
  const [dependencies, setDependencies, removeDependencies] = useLocalStorage<Dependency>(
    `dependencies-${dagId}`,
    "immediate",
  );
  const [direction, setDirection] = useLocalStorage<Direction>(`direction-${dagId}`, "RIGHT");
  const displayRunOptions = createListCollection({
    items: [
      { label: "5", value: "5" },
      { label: "10", value: "10" },
      { label: "25", value: "25" },
      { label: "50", value: "50" },
      { label: "100", value: "100" },
      { label: "365", value: "365" },
    ],
  });
  const handleLimitChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    const runLimit = Number(event.value[0]);

    setLimit(runLimit);
  };

  const handleDepsChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    if (event.value[0] === undefined || event.value[0] === "immediate" || !deps.includes(event.value[0])) {
      removeDependencies();
    } else {
      setDependencies(event.value[0]);
    }
  };

  const handleDirectionUpdate = (
    event: SelectValueChangeDetails<{ label: string; value: Array<string> }>,
  ) => {
    if (event.value[0] !== undefined) {
      setDirection(event.value[0] as Direction);
    }
  };

  return (
    <HStack
      alignItems="flex-start"
      justifyContent="space-between"
      position="absolute"
      top={0}
      width="100%"
      zIndex={1}
      {...rest}
    >
      <ButtonGroup attached size="sm" variant="outline">
        <IconButton
          aria-label="Show Grid"
          colorPalette="blue"
          onClick={() => setDagView("grid")}
          title="Show Grid"
          variant={dagView === "grid" ? "solid" : "outline"}
        >
          <FiGrid />
        </IconButton>
        <IconButton
          aria-label="Show Graph"
          colorPalette="blue"
          onClick={() => setDagView("graph")}
          title="Show Graph"
          variant={dagView === "graph" ? "solid" : "outline"}
        >
          <MdOutlineAccountTree />
        </IconButton>
      </ButtonGroup>
      <Stack alignItems="flex-end" gap={1} mr={2}>
        <HStack>
          <DagVersionSelect disabled={dagView !== "graph"} />
          <Select.Root
            bg="bg"
            collection={displayRunOptions}
            data-testid="display-dag-run-options"
            onValueChange={handleLimitChange}
            size="sm"
            value={[limit.toString()]}
            width="70px"
          >
            <Select.Trigger>
              <Select.ValueText />
            </Select.Trigger>
            <Select.Content>
              {displayRunOptions.items.map((option) => (
                <Select.Item item={option} key={option.value}>
                  {option.label}
                </Select.Item>
              ))}
            </Select.Content>
          </Select.Root>
        </HStack>
        {dagView === "graph" ? (
          <>
            <Select.Root
              bg="bg"
              collection={directionOptions}
              onValueChange={handleDirectionUpdate}
              size="sm"
              value={[direction]}
              width="150px"
            >
              <Select.Trigger>
                <Select.ValueText />
              </Select.Trigger>
              <Select.Content>
                {directionOptions.items.map((option) => (
                  <Select.Item item={option} key={option.value}>
                    {option.label}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
            <Select.Root
              bg="bg"
              collection={options}
              data-testid="filter-duration"
              onValueChange={handleDepsChange}
              size="sm"
              value={[dependencies]}
              width="210px"
            >
              <Select.Trigger>
                <Select.ValueText placeholder="Dependencies" />
              </Select.Trigger>
              <Select.Content>
                {options.items.map((option) => (
                  <Select.Item item={option} key={option.value}>
                    {option.label}
                  </Select.Item>
                ))}
              </Select.Content>
            </Select.Root>
            <DagRunSelect limit={limit} />
          </>
        ) : undefined}
      </Stack>
    </HStack>
  );
};
