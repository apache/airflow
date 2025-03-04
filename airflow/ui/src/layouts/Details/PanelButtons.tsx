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
import { useSearchParams } from "react-router-dom";

import DagVersionSelect from "src/components/DagVersionSelect";
import { Select } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";

type Props = {
  readonly dagView: string;
  readonly setDagView: (x: "graph" | "grid") => void;
} & StackProps;

const options = createListCollection({
  items: [
    { label: "Only tasks", value: "" },
    { label: "External conditions", value: "external" },
    // { label: "All Dag Dependencies", value: "all" },
  ],
});

export const PanelButtons = ({ dagView, setDagView, ...rest }: Props) => {
  const [searchParams, setSearchParams] = useSearchParams();

  const dependencies = [searchParams.get(SearchParamsKeys.DEPENDENCIES) ?? ""];

  const handleDepsChange = (event: SelectValueChangeDetails<{ label: string; value: Array<string> }>) => {
    if (event.value[0] === undefined || event.value[0] === "") {
      searchParams.delete(SearchParamsKeys.DEPENDENCIES);
    } else {
      searchParams.set(SearchParamsKeys.DEPENDENCIES, event.value[0]);
    }
    setSearchParams(searchParams);
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
        <DagVersionSelect disabled={dagView !== "graph"} />
        {dagView === "graph" ? (
          <Select.Root
            bg="bg"
            collection={options}
            data-testid="filter-duration"
            onValueChange={handleDepsChange}
            value={dependencies}
            width="200px"
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
        ) : undefined}
      </Stack>
    </HStack>
  );
};
