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
  Flex,
  IconButton,
  ButtonGroup,
  createListCollection,
  type SelectValueChangeDetails,
  Popover,
  Portal,
  Select,
} from "@chakra-ui/react";
import { useReactFlow } from "@xyflow/react";
import { FiChevronDown, FiGrid } from "react-icons/fi";
import { MdOutlineAccountTree } from "react-icons/md";
import { useParams } from "react-router-dom";
import { useLocalStorage } from "usehooks-ts";

import { DagVersionSelect } from "src/components/DagVersionSelect";
import { directionOptions, type Direction } from "src/components/Graph/useGraphLayout";
import { Button } from "src/components/ui";

import { DagRunSelect } from "./DagRunSelect";
import { ToggleGroups } from "./ToggleGroups";

type Props = {
  readonly dagView: string;
  readonly limit: number;
  readonly panelGroupRef: React.RefObject<{ setLayout?: (layout: Array<number>) => void } & HTMLDivElement>;
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

const deps = ["all", "immediate", "tasks"];

type Dependency = (typeof deps)[number];

export const PanelButtons = ({ dagView, limit, panelGroupRef, setDagView, setLimit }: Props) => {
  const { dagId = "" } = useParams();
  const { fitView } = useReactFlow();
  const [dependencies, setDependencies, removeDependencies] = useLocalStorage<Dependency>(
    `dependencies-${dagId}`,
    "tasks",
  );
  const [direction, setDirection] = useLocalStorage<Direction>(`direction-${dagId}`, "RIGHT");
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

  const handleFocus = (view: string) => {
    if (panelGroupRef.current) {
      const panelGroup = panelGroupRef.current;

      if (typeof panelGroup.setLayout === "function") {
        const newLayout = view === "graph" ? [70, 30] : [30, 70];

        panelGroup.setLayout(newLayout);
        // Used setTimeout to ensure DOM has been updated
        setTimeout(() => {
          void fitView();
        }, 1);
      }
    }
  };

  return (
    <Flex justifyContent="space-between" position="absolute" top={1} width="100%" zIndex={1}>
      <ButtonGroup attached size="sm" variant="outline">
        <IconButton
          aria-label="Show Grid"
          colorPalette="blue"
          onClick={() => {
            setDagView("grid");
            if (dagView === "grid") {
              handleFocus("grid");
            }
          }}
          title="Show Grid"
          variant={dagView === "grid" ? "solid" : "outline"}
        >
          <FiGrid />
        </IconButton>
        <IconButton
          aria-label="Show Graph"
          colorPalette="blue"
          onClick={() => {
            setDagView("graph");
            if (dagView === "graph") {
              handleFocus("graph");
            }
          }}
          title="Show Graph"
          variant={dagView === "graph" ? "solid" : "outline"}
        >
          <MdOutlineAccountTree />
        </IconButton>
      </ButtonGroup>
      <Flex gap={1} mr={3}>
        <ToggleGroups />
        {/* eslint-disable-next-line jsx-a11y/no-autofocus */}
        <Popover.Root autoFocus={false} positioning={{ placement: "bottom-end" }}>
          <Popover.Trigger asChild>
            <Button size="sm" variant="outline">
              Options
              <FiChevronDown size="0.5rem" />
            </Button>
          </Popover.Trigger>
          <Portal>
            <Popover.Positioner>
              <Popover.Content>
                <Popover.Arrow />
                <Popover.Body p={2}>
                  {dagView === "graph" ? (
                    <>
                      <DagVersionSelect />
                      <DagRunSelect limit={limit} />
                      <Select.Root
                        // @ts-expect-error The expected option type is incorrect
                        collection={options}
                        data-testid="dependencies"
                        onValueChange={handleDepsChange}
                        size="sm"
                        value={[dependencies]}
                      >
                        <Select.Label fontSize="xs">Dependencies</Select.Label>
                        <Select.Control>
                          <Select.Trigger>
                            <Select.ValueText placeholder="Dependencies" />
                          </Select.Trigger>
                          <Select.IndicatorGroup>
                            <Select.Indicator />
                          </Select.IndicatorGroup>
                        </Select.Control>
                        <Select.Positioner>
                          <Select.Content>
                            {options.items.map((option) => (
                              <Select.Item item={option} key={option.value}>
                                {option.label}
                              </Select.Item>
                            ))}
                          </Select.Content>
                        </Select.Positioner>
                      </Select.Root>
                      <Select.Root
                        // @ts-expect-error The expected option type is incorrect
                        collection={directionOptions}
                        onValueChange={handleDirectionUpdate}
                        size="sm"
                        value={[direction]}
                      >
                        <Select.Label fontSize="xs">Graph Direction</Select.Label>
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
                            {directionOptions.items.map((option) => (
                              <Select.Item item={option} key={option.value}>
                                {option.label}
                              </Select.Item>
                            ))}
                          </Select.Content>
                        </Select.Positioner>
                      </Select.Root>
                    </>
                  ) : (
                    <Select.Root
                      // @ts-expect-error The expected option type is incorrect
                      collection={displayRunOptions}
                      data-testid="display-dag-run-options"
                      onValueChange={handleLimitChange}
                      size="sm"
                      value={[limit.toString()]}
                    >
                      <Select.Label>Number of Dag Runs</Select.Label>
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
                          {displayRunOptions.items.map((option) => (
                            <Select.Item item={option} key={option.value}>
                              {option.label}
                            </Select.Item>
                          ))}
                        </Select.Content>
                      </Select.Positioner>
                    </Select.Root>
                  )}
                </Popover.Body>
              </Popover.Content>
            </Popover.Positioner>
          </Portal>
        </Popover.Root>
      </Flex>
    </Flex>
  );
};
