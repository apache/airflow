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

import React, { useEffect } from "react";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  Text,
} from "@chakra-ui/react";

import { getDagRunLabel, getMetaValue, getTask } from "src/utils";
import useSelection from "src/dag/useSelection";
import Time from "src/components/Time";
import { useGridData } from "src/api";
import RunTypeIcon from "src/components/RunTypeIcon";
import BreadcrumbText from "src/components/BreadcrumbText";

const dagDisplayName = getMetaValue("dag_display_name");

interface Props {
  mapIndex?: string | number | null;
}

const Header = ({ mapIndex }: Props) => {
  const {
    data: { dagRuns, groups },
  } = useGridData();

  const {
    selected: { taskId, runId },
    onSelect,
    clearSelection,
  } = useSelection();

  const dagRun = dagRuns.find((r) => r.runId === runId);
  const group = getTask({ taskId, task: groups });

  // If taskId can't be found remove the selection
  useEffect(() => {
    if (taskId && !group) {
      onSelect({ runId });
    }
  }, [taskId, group, onSelect, runId]);

  let runLabel;
  if (dagRun && runId) {
    // If a runId includes the runtype then parse the time, otherwise use the custom run id
    const runName =
      runId.includes("manual__") ||
      runId.includes("scheduled__") ||
      runId.includes("backfill__") ||
      runId.includes("dataset_triggered__") ? (
        <Time dateTime={getDagRunLabel({ dagRun })} />
      ) : (
        runId
      );
    runLabel = (
      <>
        <RunTypeIcon runType={dagRun.runType} />
        {runName}
      </>
    );
  }

  const taskName = group?.label || group?.id || "";

  const isDagDetails = !runId && !taskId;
  const isRunDetails = !!(runId && !taskId);
  const isTaskDetails = !runId && taskId;
  const isMappedTaskDetails = runId && taskId && mapIndex !== undefined;

  return (
    <Breadcrumb ml={3} pt={2} separator={<Text color="gray.300">/</Text>}>
      <BreadcrumbItem isCurrentPage={isDagDetails} mt={4}>
        <BreadcrumbLink
          onClick={clearSelection}
          _hover={isDagDetails ? { cursor: "default" } : undefined}
        >
          <BreadcrumbText label="DAG" value={dagDisplayName} />
        </BreadcrumbLink>
      </BreadcrumbItem>
      {runId && (
        <BreadcrumbItem isCurrentPage={isRunDetails} mt={4}>
          <BreadcrumbLink
            onClick={() => onSelect({ runId })}
            _hover={isRunDetails ? { cursor: "default" } : undefined}
          >
            <BreadcrumbText label="Run" value={runLabel} />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
      {taskId && (
        <BreadcrumbItem isCurrentPage mt={4}>
          <BreadcrumbLink
            onClick={() =>
              mapIndex !== undefined
                ? onSelect({ runId, taskId })
                : onSelect({ taskId })
            }
            _hover={isTaskDetails ? { cursor: "default" } : undefined}
          >
            <BreadcrumbText
              label="Task"
              value={`${taskName}${group?.isMapped ? " []" : ""}`}
            />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
      {mapIndex !== undefined && mapIndex !== -1 && (
        <BreadcrumbItem isCurrentPage mt={4}>
          <BreadcrumbLink
            _hover={isMappedTaskDetails ? { cursor: "default" } : undefined}
          >
            <BreadcrumbText label="Map Index" value={mapIndex} />
          </BreadcrumbLink>
        </BreadcrumbItem>
      )}
    </Breadcrumb>
  );
};

export default Header;
