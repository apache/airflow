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
  } else if (!xcom || !xcom.value) {
    content = (
      <Alert status="info">
        <AlertIcon />
        No value found for XCom key
      </Alert>
    );
  } else {
    // Note:
    // The Airflow API delivers the XCom value as Python JSON dump
    // with Python style quotes - which can not be parsed in JavaScript
    // by default.
    // Example: {'key': 'value'}
    // JavaScript expects: {"key": "value"}
    // So we attempt to replaces string quotes which in 90% of cases will work
    // It will fail if embedded quotes are in quotes. Then the XCom will be
    // rendered as string as fallback. Better ideas welcome here.
    const xcomString = xcom.value.replace(/'/g, '"');
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
