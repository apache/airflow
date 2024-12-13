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

import {
  useDagRunServiceGetDagRunsKey,
  useDagRunServiceTriggerDagRun,
  useDagServiceGetDagsKey,
  useDagsServiceRecentDagRunsKey,
} from "openapi/queries";
import type { DagRunTriggerParams } from "src/components/TriggerDag/TriggerDAGForm";
import { toaster } from "src/components/ui";

const onError = () => {
  toaster.create({
    description:
      "Failed to initiate the DAG run request. An unexpected error occurred.",
    title: "Dag Run Request Failed",
    type: "error",
  });
};

export const useTrigger = () => {
  const queryClient = useQueryClient();

  const onSuccess = async () => {
    // Invalidate necessary queries
    await queryClient.invalidateQueries({
      queryKey: [useDagServiceGetDagsKey],
    });

    await queryClient.invalidateQueries({
      queryKey: [useDagsServiceRecentDagRunsKey],
    });

    await queryClient.invalidateQueries({
      queryKey: [useDagRunServiceGetDagRunsKey],
    });

    // Show success toaster
    toaster.create({
      description: "DAG run has been successfully triggered.",
      title: "DAG Run Request Submitted",
      type: "success",
    });
  };

  const { isPending, mutate } = useDagRunServiceTriggerDagRun({
    onError,
    onSuccess,
  });

  // Function to trigger a Dag run
  const triggerDagRun = (
    dagId: string,
    dagRunRequestBody: DagRunTriggerParams,
  ) => {
    const parsedConfig = JSON.parse(dagRunRequestBody.conf) as Record<
      string,
      unknown
    >;

    // Validate and format data intervals
    const formattedDataIntervalStart = dagRunRequestBody.dataIntervalStart
      ? new Date(dagRunRequestBody.dataIntervalStart).toISOString()
      : undefined;
    const formattedDataIntervalEnd = dagRunRequestBody.dataIntervalEnd
      ? new Date(dagRunRequestBody.dataIntervalEnd).toISOString()
      : undefined;

    // Call mutate with the required parameters
    mutate({
      dagId,
      requestBody: {
        conf: parsedConfig,
        dag_run_id: dagRunRequestBody.dagRunId,
        data_interval_end: formattedDataIntervalEnd,
        data_interval_start: formattedDataIntervalStart,
        note: dagRunRequestBody.note,
      },
    });
  };

  return { isPending, triggerDagRun };
};
