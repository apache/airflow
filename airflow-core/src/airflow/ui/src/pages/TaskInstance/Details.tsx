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
import { Box, Flex, HStack, Table } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams, useSearchParams } from "react-router-dom";

import {
  useTaskInstanceServiceGetMappedTaskInstance,
  useTaskInstanceServiceGetTaskInstanceTryDetails,
} from "openapi/queries";
import { DagVersionDetails } from "src/components/DagVersionDetails";
import { StateBadge } from "src/components/StateBadge";
import { TaskTrySelect } from "src/components/TaskTrySelect";
import Time from "src/components/Time";
import { ClipboardRoot, ClipboardIconButton } from "src/components/ui";
import { SearchParamsKeys } from "src/constants/searchParams";
import { useAutoRefresh, isStatePending, renderDuration } from "src/utils";

import { BlockingDeps } from "./BlockingDeps";
import { ExtraLinks } from "./ExtraLinks";
import { TriggererInfo } from "./TriggererInfo";

export const Details = () => {
  const { t: translate } = useTranslation();
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();

  const tryNumberParam = searchParams.get(SearchParamsKeys.TRY_NUMBER);
  const parsedMapIndex = parseInt(mapIndex, 10);

  const { data: taskInstance } = useTaskInstanceServiceGetMappedTaskInstance(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parsedMapIndex,
      taskId,
    },
    undefined,
    {
      enabled: !isNaN(parsedMapIndex),
    },
  );

  const onSelectTryNumber = (newTryNumber: number) => {
    if (newTryNumber === taskInstance?.try_number) {
      searchParams.delete(SearchParamsKeys.TRY_NUMBER);
    } else {
      searchParams.set(SearchParamsKeys.TRY_NUMBER, newTryNumber.toString());
    }
    setSearchParams(searchParams);
  };

  const tryNumber = tryNumberParam === null ? taskInstance?.try_number : parseInt(tryNumberParam, 10);

  const refetchInterval = useAutoRefresh({ dagId });

  const { data: tryInstance } = useTaskInstanceServiceGetTaskInstanceTryDetails(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
      taskTryNumber: tryNumber ?? 1,
    },
    undefined,
    {
      refetchInterval: (query) => (isStatePending(query.state.data?.state) ? refetchInterval : false),
    },
  );

  return (
    <Box p={2}>
      {taskInstance === undefined || tryNumber === undefined || taskInstance.try_number <= 1 ? (
        <div />
      ) : (
        <TaskTrySelect
          onSelectTryNumber={onSelectTryNumber}
          selectedTryNumber={tryNumber}
          taskInstance={taskInstance}
        />
      )}
      <ExtraLinks />
      {taskInstance === undefined ||
      // eslint-disable-next-line unicorn/no-null
      ![null, "queued", "scheduled"].includes(taskInstance.state) ? undefined : (
        <BlockingDeps taskInstance={taskInstance} />
      )}
      {taskInstance !== undefined && (taskInstance.trigger ?? taskInstance.triggerer_job) ? (
        <TriggererInfo taskInstance={taskInstance} />
      ) : undefined}
      <Table.Root striped>
        <Table.Body>
          <Table.Row>
            <Table.Cell>{translate("state")}</Table.Cell>
            <Table.Cell>
              <Flex gap={1}>
                <StateBadge state={tryInstance?.state} />
                {tryInstance?.state ?? translate("states.no_status")}
              </Flex>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskId")}</Table.Cell>
            <Table.Cell>
              <HStack>
                {tryInstance?.task_id}
                <ClipboardRoot value={tryInstance?.task_id}>
                  <ClipboardIconButton />
                </ClipboardRoot>
              </HStack>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("runId")}</Table.Cell>
            <Table.Cell>
              <HStack>
                {tryInstance?.dag_run_id}
                <ClipboardRoot value={tryInstance?.dag_run_id}>
                  <ClipboardIconButton />
                </ClipboardRoot>
              </HStack>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("mapIndex")}</Table.Cell>
            <Table.Cell>{tryInstance?.map_index}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("task.operator")}</Table.Cell>
            <Table.Cell>{tryInstance?.operator_name}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("duration")}</Table.Cell>
            <Table.Cell>{renderDuration(tryInstance?.duration)}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("startDate")}</Table.Cell>
            <Table.Cell>
              <Time datetime={tryInstance?.start_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("endDate")}</Table.Cell>
            <Table.Cell>
              <Time datetime={tryInstance?.end_date} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.dagVersion")}</Table.Cell>
            <Table.Cell>
              <DagVersionDetails dagVersion={taskInstance?.dag_version} />
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.pid")}</Table.Cell>
            <Table.Cell>
              <HStack>
                {tryInstance?.pid}
                <ClipboardRoot value={String(tryInstance?.pid ?? "")}>
                  <ClipboardIconButton />
                </ClipboardRoot>
              </HStack>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.hostname")}</Table.Cell>
            <Table.Cell>
              <HStack>
                {tryInstance?.hostname}
                <ClipboardRoot value={tryInstance?.hostname ?? ""}>
                  <ClipboardIconButton />
                </ClipboardRoot>
              </HStack>
            </Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.unixname")}</Table.Cell>
            <Table.Cell>{tryInstance?.unixname}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.pool")}</Table.Cell>
            <Table.Cell>{tryInstance?.pool}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.queue")}</Table.Cell>
            <Table.Cell>{tryInstance?.queue}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.priorityWeight")}</Table.Cell>
            <Table.Cell>{tryInstance?.priority_weight}</Table.Cell>
          </Table.Row>
          <Table.Row>
            <Table.Cell>{translate("taskInstance.executor")}</Table.Cell>
            <Table.Cell>{tryInstance?.executor_config}</Table.Cell>
          </Table.Row>
        </Table.Body>
      </Table.Root>
    </Box>
  );
};
