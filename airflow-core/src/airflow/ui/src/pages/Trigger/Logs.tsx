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
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTriggerServiceGetTrigger } from "openapi/queries";
import type { TriggerDetailResponse } from "openapi/requests/types.gen";

import { Alert, RouterLink } from "src/system-components";

import { LogViewer, useLogDisplayOptions } from "src/pages/Run/CallbackLogs";

import { useTriggerLogs } from "src/queries/useLogs";
import { getErrorStatus } from "src/utils";
import { getTaskInstanceLink } from "src/utils/links";

/** The owner's full log, for a trigger whose lines are part of it; a watcher's lines are all it has. */
const getOwnerLogLink = ({ callback, task_instance: taskInstance }: TriggerDetailResponse) => {
  if (taskInstance !== null) {
    return getTaskInstanceLink(
      {
        dagId: taskInstance.dag_id,
        dagRunId: taskInstance.dag_run_id,
        mapIndex: taskInstance.map_index,
        taskId: taskInstance.task_id,
      },
      "logs",
    );
  }

  return callback !== null && callback.dag_id !== null && callback.dag_run_id !== null
    ? `/dags/${callback.dag_id}/runs/${callback.dag_run_id}/callbacks`
    : undefined;
};

export const Logs = () => {
  const { t: translate } = useTranslation(["dag", "common"]);
  const { triggerId = "" } = useParams();
  const display = useLogDisplayOptions();
  const { data: trigger } = useTriggerServiceGetTrigger({ triggerId: Number(triggerId) });
  const ownerLogLink = trigger === undefined ? undefined : getOwnerLogLink(trigger);

  const { error, fetchedData, isLoading, parsedData } = useTriggerLogs({
    logLevelFilters: display.logLevelFilters,
    showLogLevel: display.showLogLevel,
    showSource: display.showSource,
    showTimestamp: display.showTimestamp,
    sourceFilters: display.sourceFilters,
    triggerId: Number(triggerId),
  });

  if (getErrorStatus(error) === 403) {
    return (
      <Alert status="warning" title={translate("common:errors.forbidden.title")}>
        {translate("logs.trigger.forbidden")}
      </Alert>
    );
  }

  return (
    <Box display="flex" flexDirection="column" h="70vh">
      {ownerLogLink === undefined ? undefined : (
        <RouterLink alignSelf="start" fontSize="sm" mb={2} to={ownerLogLink}>
          {translate("logs.trigger.openOwnerLog")}
        </RouterLink>
      )}
      <LogViewer
        display={display}
        downloadName={`logs_trigger_${triggerId}.txt`}
        // The page says so when the trigger is gone, as once it has fired.
        error={getErrorStatus(error) === 404 ? undefined : error}
        fetchedData={fetchedData}
        isLoading={isLoading}
        parsedData={parsedData}
        title={`${translate("common:trigger_one")} ${triggerId}`}
      />
    </Box>
  );
};
