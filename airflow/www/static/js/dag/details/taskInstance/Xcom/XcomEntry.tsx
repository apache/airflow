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

import { Alert, AlertIcon, Spinner, Td, Tr } from "@chakra-ui/react";
import React from "react";

import { useTaskXcomEntry } from "src/api";
import ErrorAlert from "src/components/ErrorAlert";
import RenderedJsonField from "src/components/RenderedJsonField";
import type { Dag, DagRun, TaskInstance } from "src/types";

interface Props {
  dagId: Dag["id"];
  dagRunId: DagRun["runId"];
  taskId: TaskInstance["taskId"];
  mapIndex?: TaskInstance["mapIndex"];
  xcomKey: string;
  tryNumber: TaskInstance["tryNumber"];
}

const XcomEntry = ({
  dagId,
  dagRunId,
  taskId,
  mapIndex,
  xcomKey,
  tryNumber,
}: Props) => {
  const {
    data: xcom,
    isLoading,
    error,
  } = useTaskXcomEntry({
    dagId,
    dagRunId,
    taskId,
    mapIndex,
    xcomKey,
    tryNumber: tryNumber || 1,
  });

  let content = null;
  if (isLoading) {
    content = <Spinner />;
  } else if (error) {
    content = <ErrorAlert error={error} />;
  } else if (!xcom) {
    content = (
      <Alert status="info">
        <AlertIcon />
        No value found for XCom key
      </Alert>
    );
  } else if (xcom.value === undefined || xcom.value === null) {
    content = (
      <Alert status="info">
        <AlertIcon />
        Value is NULL
      </Alert>
    );
  } else {
    let xcomString = "";
    if (typeof xcom.value !== "string") {
      try {
        xcomString = JSON.stringify(xcom.value);
      } catch (e) {
        // skip
      }
    } else {
      xcomString = xcom.value as string;
    }
    content = <RenderedJsonField content={xcomString} />;
  }

  return (
    <Tr>
      <Td>{xcomKey}</Td>
      <Td>{content}</Td>
    </Tr>
  );
};

export default XcomEntry;
