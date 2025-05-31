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
import { useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import {
  UseDagRunServiceGetDagRunsKeyFn,
  useDagRunServiceTriggerDagRun,
  useDagServiceGetDagsKey,
  useDagServiceRecentDagRunsKey,
  UseGridServiceGridDataKeyFn,
  UseTaskInstanceServiceGetTaskInstancesKeyFn,
} from "openapi/queries";
import type { DagRunTriggerParams } from "src/components/TriggerDag/TriggerDAGForm";
import { toaster } from "src/components/ui";

export const useTrigger = ({ dagId, onSuccessConfirm }: { dagId: string; onSuccessConfirm: () => void }) => {
  const queryClient = useQueryClient();
  const [error, setError] = useState<unknown>(undefined);

  const onSuccess = async () => {
    const queryKeys = [
      [useDagServiceGetDagsKey],
      [useDagServiceRecentDagRunsKey],
      UseDagRunServiceGetDagRunsKeyFn({ dagId }, [{ dagId }]),
      UseTaskInstanceServiceGetTaskInstancesKeyFn({ dagId, dagRunId: "~" }, [{ dagId, dagRunId: "~" }]),
      UseGridServiceGridDataKeyFn({ dagId }, [{ dagId }]),
    ];

    await Promise.all(queryKeys.map((key) => queryClient.invalidateQueries({ queryKey: key })));

    toaster.create({
      description: "DAG run has been successfully triggered.",
      title: "DAG Run Request Submitted",
      type: "success",
    });
    onSuccessConfirm();
  };

  const onError = (_error: unknown) => {
    setError(_error);
  };

  const { isPending, mutate } = useDagRunServiceTriggerDagRun({
    onError,
    onSuccess,
  });

  const triggerDagRun = (dagRunRequestBody: DagRunTriggerParams) => {
    const parsedConfig = JSON.parse(dagRunRequestBody.conf) as Record<string, unknown>;

    const logicalDate = dagRunRequestBody.logicalDate ? new Date(dagRunRequestBody.logicalDate) : undefined;

    // eslint-disable-next-line unicorn/no-null
    const formattedLogicalDate = logicalDate?.toISOString() ?? null;

    const checkDagRunId = dagRunRequestBody.dagRunId === "" ? undefined : dagRunRequestBody.dagRunId;
    const checkNote = dagRunRequestBody.note === "" ? undefined : dagRunRequestBody.note;

    mutate({
      dagId,
      requestBody: {
        conf: parsedConfig,
        dag_run_id: checkDagRunId,
        logical_date: formattedLogicalDate,
        note: checkNote,
      },
    });
  };

  return { error, isPending, triggerDagRun };
};
