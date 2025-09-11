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
import { Button } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { useParams } from "react-router-dom";

import { useTaskInstanceServiceGetExternalLogUrl } from "openapi/queries";
import type { TaskInstanceResponse } from "openapi/requests/types.gen";

type Props = {
  readonly externalLogName: string;
  readonly taskInstance: TaskInstanceResponse;
  readonly tryNumber: number;
};

export const ExternalLogLink = ({ externalLogName, taskInstance, tryNumber }: Props) => {
  const { t: translate } = useTranslation("dag");
  const { dagId = "", mapIndex = "-1", runId = "", taskId = "" } = useParams();

  const {
    data: externalLogData,
    error,
    isLoading,
  } = useTaskInstanceServiceGetExternalLogUrl(
    {
      dagId,
      dagRunId: runId,
      mapIndex: parseInt(mapIndex, 10),
      taskId,
      tryNumber,
    },
    undefined,
    {
      enabled: Boolean(taskInstance) && Boolean(tryNumber) && Boolean(externalLogName),
      retry: false,
    },
  );

  if (Boolean(error) || isLoading || externalLogData?.url === undefined) {
    return undefined;
  }

  return (
    <Button asChild colorScheme="brand" variant="outline">
      <a href={externalLogData.url} rel="noopener noreferrer" target="_blank">
        {translate("logs.viewInExternal", { attempt: tryNumber, name: externalLogName })}
      </a>
    </Button>
  );
};
