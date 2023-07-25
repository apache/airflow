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

import { getMetaValue } from "src/utils";
import LinkButton from "src/components/LinkButton";
import type { Dag, DagRun, TaskInstance } from "src/types";

const logsWithMetadataUrl = getMetaValue("logs_with_metadata_url");
const externalLogUrl = getMetaValue("external_log_url");

interface Props {
  dagId: Dag["id"];
  taskId: TaskInstance["taskId"];
  executionDate: DagRun["executionDate"];
  isInternal?: boolean;
  tryNumber: TaskInstance["tryNumber"];
  mapIndex?: TaskInstance["mapIndex"];
}

const LogLink = ({
  dagId,
  taskId,
  executionDate,
  isInternal,
  tryNumber,
  mapIndex,
}: Props) => {
  let fullMetadataUrl = `${
    isInternal ? logsWithMetadataUrl : externalLogUrl
  }?dag_id=${encodeURIComponent(dagId)}&task_id=${encodeURIComponent(
    taskId
  )}&execution_date=${encodeURIComponent(
    executionDate
  )}&map_index=${encodeURIComponent(mapIndex?.toString() ?? "-1")}`;

  if (isInternal && tryNumber) {
    fullMetadataUrl += `&format=file${
      tryNumber > 0 && `&try_number=${tryNumber}`
    }`;
  } else {
    fullMetadataUrl += `&try_number=${tryNumber}`;
  }
  return (
    <LinkButton
      href={fullMetadataUrl}
      target={isInternal ? undefined : "_blank"}
    >
      {isInternal ? "Download" : tryNumber}
    </LinkButton>
  );
};

export default LogLink;
