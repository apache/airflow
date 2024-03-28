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
import { MdDetails, MdOutlineViewTimeline } from "react-icons/md";

import type { DagRun } from "src/types";

import BreadcrumbText from "./BreadcrumbText";
import TabButton from "./TabButton";
import ClearRun from "./dagRun/ClearRun";
import MarkRunAs from "./dagRun/MarkRunAs";

interface Props {
  runId: string;
  tab?: string;
  onChangeTab: (nextTab: string) => void;
  run?: DagRun;
}

const RunButtons = ({ run, runId, tab, onChangeTab }: Props) => (
  <Flex
    pl={10}
    borderBottomWidth={1}
    borderBottomColor="gray.200"
    alignItems="center"
    justifyContent="space-between"
  >
    <Flex alignItems="flex-end">
      <BreadcrumbText label="Run" value={runId} />
      <TabButton
        isActive={tab === "gantt"}
        onClick={() => onChangeTab("gantt")}
      >
        <MdOutlineViewTimeline size={16} />
        <Text as="strong" ml={1}>
          Gantt
        </Text>
      </TabButton>
      <TabButton
        isActive={tab === "run_details"}
        onClick={() => onChangeTab("run_details")}
      >
        <MdDetails size={16} />
        <Text as="strong" ml={1}>
          Run Details
        </Text>
      </TabButton>
    </Flex>
    <Flex>
      <ClearRun runId={runId} mr={2} />
      <MarkRunAs runId={runId} state={run?.state} />
    </Flex>
  </Flex>
);

export default RunButtons;
