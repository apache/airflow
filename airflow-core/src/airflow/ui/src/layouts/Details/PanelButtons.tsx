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
  Box,
  IconButton,
  ButtonGroup,
  Stack,
  createListCollection,
  type SelectValueChangeDetails,
  Accordion,
  Text,
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
  readonly setLimit: React.Dispatch<React.SetStateAction<number>>;
};

const options = createListCollection({
  items: [
    { label: "Only tasks", value: "tasks" },
    { label: "External conditions", value: "immediate" },
    { label: "All Dag Dependencies", value: "all" },
  ],
});

const deps = ["all", "immediate", "tasks"];

type Dependency = (typeof deps)[number];

export const PanelButtons = ({ dagView, limit, setDagView, setLimit }: Props) => {
  const { dagId = "" } = useParams();
  const [dependencies, setDependencies, removeDependencies] = useLocalStorage<Dependency>(
    `dependencies-${dagId}`,
    "tasks",
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
    if (event.value[0] === undefined || event.value[0] === "tasks" || !deps.includes(event.value[0])) {
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
    <>
      <ButtonGroup attached left={0} position="absolute" size="sm" top={0} variant="outline" zIndex={1}>
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
      <Box justifyContent="flex-end" position="absolute" right={3} top={0} width="250px" zIndex={1}>
        <Accordion.Root collapsible>
          <Accordion.Item borderBottomWidth={0} value="1">
            <Accordion.ItemTrigger justifyContent="flex-end">
              <Text fontSize="sm">Options</Text>
              <Accordion.ItemIndicator />
            </Accordion.ItemTrigger>
            <Accordion.ItemContent display="flex">
              <Accordion.ItemBody bg="bg.muted" p={2} width="fit-content">
                <Stack gap={1} mr={2}>
                  {dagView === "graph" ? (
                    <>
                      <DagVersionSelect />
                      <DagRunSelect limit={limit} />
                      <Select.Root
                        collection={options}
                        data-testid="filter-duration"
                        onValueChange={handleDepsChange}
                        size="sm"
                        value={[dependencies]}
                      >
                        <Select.Label fontSize="xs">Dependencies</Select.Label>
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
                      <Select.Root
                        collection={directionOptions}
                        onValueChange={handleDirectionUpdate}
                        size="sm"
                        value={[direction]}
                      >
                        <Select.Label fontSize="xs">Graph Direction</Select.Label>
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
                    </>
                  ) : (
                    <Select.Root
                      collection={displayRunOptions}
                      data-testid="display-dag-run-options"
                      onValueChange={handleLimitChange}
                      size="sm"
                      value={[limit.toString()]}
                    >
                      <Select.Label>Number of Dag Runs</Select.Label>
                      <Select.Trigger>
                        {}
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
                  )}
                </Stack>
              </Accordion.ItemBody>
            </Accordion.ItemContent>
          </Accordion.Item>
        </Accordion.Root>
      </Box>
    </>
  );
};
