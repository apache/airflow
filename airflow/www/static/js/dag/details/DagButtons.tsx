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
  MdAccountTree,
  MdCode,
  MdHourglassBottom,
  MdPlagiarism,
  MdEvent,
} from "react-icons/md";

import BreadcrumbText from "./BreadcrumbText";
import TabButton from "./TabButton";

interface Props {
  dagId: string;
  tab?: string;
  onChangeTab: (newTab: string) => void;
}

const DagButtons = ({ dagId, tab, onChangeTab }: Props) => (
  <Flex
    pl={10}
    // pt={8}
    borderBottomWidth={1}
    borderBottomColor="gray.200"
    alignItems="flex-end"
  >
    <BreadcrumbText label="DAG" value={dagId} />

    <TabButton isActive={tab === "graph"} onClick={() => onChangeTab("graph")}>
      <MdAccountTree size={16} />
      <Text as="strong" ml={1}>
        Graph
      </Text>
    </TabButton>
    <TabButton isActive={tab === "code"} onClick={() => onChangeTab("code")}>
      <MdCode size={16} />
      <Text as="strong" ml={1}>
        Code
      </Text>
    </TabButton>
    <TabButton
      isActive={tab === "audit_log"}
      onClick={() => onChangeTab("audit_log")}
    >
      <MdPlagiarism size={16} />
      <Text as="strong" ml={1}>
        Audit Log
      </Text>
    </TabButton>
    <TabButton
      isActive={tab === "run_duration"}
      onClick={() => onChangeTab("run_duration")}
    >
      <MdHourglassBottom size={16} />
      <Text as="strong" ml={1}>
        Run Duration
      </Text>
    </TabButton>
    <TabButton
      isActive={tab === "calendar"}
      onClick={() => onChangeTab("calendar")}
    >
      <MdEvent size={16} />
      <Text as="strong" ml={1}>
        Calendar
      </Text>
    </TabButton>
    <TabButton
      isActive={tab === "dag_details"}
      onClick={() => onChangeTab("dag_details")}
    >
      <MdDetails size={16} />
      <Text as="strong" ml={1}>
        DAG Details
      </Text>
    </TabButton>
  </Flex>
);

export default DagButtons;
