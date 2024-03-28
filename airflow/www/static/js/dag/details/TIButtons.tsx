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

import React from "react";
import { Flex, Text } from "@chakra-ui/react";
import {
  MdDetails,
  MdHourglassBottom,
  MdSyncAlt,
  MdReorder,
} from "react-icons/md";
import { BiBracket } from "react-icons/bi";

import type { DagRun, TaskState } from "src/types";

import BreadcrumbText from "./BreadcrumbText";
import TabButton from "./TabButton";
import ClearInstance from "./taskInstance/taskActions/ClearInstance";
import MarkInstanceAs from "./taskInstance/taskActions/MarkInstanceAs";
import FilterTasks from "./FilterTasks";

interface Props {
  runId?: string | null;
  taskId: string;
  renderedMapIndex?: string | number;
  tab?: string;
  onChangeTab: (nextTab: string) => void;
  isTaskInstance?: boolean;
  isMappedTaskSummary: boolean;
  isAbandonedTask?: boolean;
  run?: DagRun;
  isMapped?: boolean;
  isGroup?: boolean;
  mapIndex?: number;
  state?: TaskState;
}

const TIButtons = ({
  runId,
  taskId,
  renderedMapIndex,
  tab,
  onChangeTab,
  isTaskInstance,
  isMappedTaskSummary,
  isAbandonedTask,
  run,
  isGroup,
  isMapped,
  mapIndex,
  state,
}: Props) => {
  let text = taskId;
  if (isMappedTaskSummary) {
    text = `${taskId} []`;
  }
  if (renderedMapIndex !== undefined) {
    text = `${taskId} [${renderedMapIndex}]`;
  }

  return (
    <Flex
      pl={10}
      borderBottomWidth={1}
      borderBottomColor="gray.200"
      justifyContent="space-between"
      alignItems="center"
    >
      <Flex alignItems="flex-end">
        <BreadcrumbText label="Task" value={text} />
        {runId && isTaskInstance && (
          <TabButton
            isActive={tab === "logs"}
            onClick={() => onChangeTab("logs")}
          >
            <MdReorder size={16} />
            <Text as="strong" ml={1}>
              Logs
            </Text>
          </TabButton>
        )}
        {runId && isTaskInstance && (
          <TabButton
            isActive={tab === "xcom"}
            onClick={() => onChangeTab("xcom")}
          >
            <MdSyncAlt size={16} />
            <Text as="strong" ml={1}>
              XCom
            </Text>
          </TabButton>
        )}
        {(isMappedTaskSummary || renderedMapIndex !== undefined) && (
          <TabButton
            isActive={tab === "mapped_tasks"}
            onClick={() => onChangeTab("mapped_tasks")}
          >
            <BiBracket size={16} />
            <Text as="strong" ml={1}>
              Mapped Tasks
            </Text>
          </TabButton>
        )}
        {runId && (
          <TabButton
            isActive={tab === "task_details"}
            onClick={() => onChangeTab("task_details")}
          >
            <MdDetails size={16} />
            <Text as="strong" ml={1}>
              Task Details
            </Text>
          </TabButton>
        )}
        <TabButton
          isActive={tab === "task_duration"}
          onClick={() => onChangeTab("task_duration")}
        >
          <MdHourglassBottom size={16} />
          <Text as="strong" ml={1}>
            Task Duration
          </Text>
        </TabButton>
      </Flex>
      <Flex>
        {runId && taskId && !isAbandonedTask && (
          <>
            <ClearInstance
              taskId={taskId}
              runId={runId}
              executionDate={run?.executionDate || ""}
              isGroup={isGroup}
              isMapped={isMapped}
              mapIndex={mapIndex}
              mt={2}
              mr={2}
            />
            <MarkInstanceAs
              taskId={taskId}
              runId={runId}
              state={state}
              isGroup={isGroup}
              isMapped={isMapped}
              mapIndex={mapIndex}
              mt={2}
              mr={2}
            />
          </>
        )}
        {taskId && runId && <FilterTasks taskId={taskId} />}
      </Flex>
    </Flex>
  );
};

export default TIButtons;
