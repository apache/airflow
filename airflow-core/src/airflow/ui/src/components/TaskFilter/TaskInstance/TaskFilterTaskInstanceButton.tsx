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
import { Box } from "@chakra-ui/react";
import { MdFilterList } from "react-icons/md";
import { useLocalStorage } from "usehooks-ts";

import type { TaskInstanceResponse } from "openapi/requests";
import { Menu } from "src/components/ui";
import ActionButton from "src/components/ui/ActionButton";

type Props = {
  readonly taskInstance: TaskInstanceResponse;
  readonly withText?: boolean;
};

const filterOptions = [
  { buttonLabel: "Filter Dag by task", label: "All", value: "all" },
  { buttonLabel: "Only upstream", label: "Only upstream", value: "upstream" },
  { buttonLabel: "Only downstream", label: "Only downstream", value: "downstream" },
  { buttonLabel: "Both upstream & downstream", label: "Both upstream & downstream", value: "both" },
];

const TaskFilterTaskInstanceButton = ({ taskInstance, withText = true }: Props) => {
  const dagId = taskInstance.dag_id;
  const [selected, setSelected] = useLocalStorage<string>(`upstreamDownstreamFilter-${dagId}`, "all");

  const handleSelect = (value: string) => {
    setSelected(value);
  };

  return (
    <Box>
      <Menu.Root positioning={{ gutter: 0, placement: "bottom" }}>
        <Menu.Trigger asChild>
          <ActionButton
            actionName={`Filter: ${filterOptions.find((opt) => opt.value === selected)?.buttonLabel}`}
            flexDirection="row-reverse"
            icon={<MdFilterList />}
            text={filterOptions.find((opt) => opt.value === selected)?.buttonLabel ?? ""}
            withText={withText}
          />
        </Menu.Trigger>

        <Menu.Content>
          {filterOptions.map((option) => (
            <Menu.Item
              asChild
              disabled={selected === option.value}
              key={option.value}
              onClick={() => handleSelect(option.value)}
              value={option.value}
            >
              <div className="px-2 py-1 cursor-pointer">{option.label}</div>
            </Menu.Item>
          ))}
        </Menu.Content>
      </Menu.Root>
    </Box>
  );
};

export default TaskFilterTaskInstanceButton;
