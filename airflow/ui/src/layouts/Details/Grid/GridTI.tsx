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
import { Flex } from "@chakra-ui/react";
import type { MouseEvent } from "react";
import React from "react";

import type { TaskInstanceState } from "openapi/requests/types.gen";

import { GridButton } from "./GridButton";

type Props = {
  readonly dagId: string;
  readonly isGroup?: boolean;
  readonly label: string;
  readonly runId: string;
  readonly search: string;
  readonly state?: TaskInstanceState | null;
  readonly taskId: string;
};

const onMouseEnter = (event: MouseEvent<HTMLDivElement>) => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#name-${event.currentTarget.id}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "var(--chakra-colors-blue-subtle)";
  });
};

const onMouseLeave = (event: MouseEvent<HTMLDivElement>) => {
  const tasks = document.querySelectorAll<HTMLDivElement>(`#name-${event.currentTarget.id}`);

  tasks.forEach((task) => {
    task.style.backgroundColor = "";
  });
};

const Instance = ({ dagId, isGroup, label, runId, search, state, taskId }: Props) => (
  <Flex
    alignItems="flex-end"
    id={taskId.replaceAll(".", "-")}
    justifyContent="center"
    key={taskId}
    onMouseEnter={onMouseEnter}
    onMouseLeave={onMouseLeave}
    px="5px"
    py="5px"
    transition="background-color 0.2s"
    width="14px"
    zIndex={1}
  >
    <GridButton
      dagId={dagId}
      isGroup={isGroup}
      label={label}
      p="2px"
      runId={runId}
      searchParams={search}
      state={state}
      taskId={taskId}
    />
  </Flex>
);

export const GridTI = React.memo(Instance);
