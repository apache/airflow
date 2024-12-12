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
import { useDagRunServiceTriggerDagRun } from "openapi/queries";
import type { TriggerDAGRunPostBody } from "openapi/requests/types.gen";

export type DagParams = {
  configJson: string;
  dagId: string;
  dataIntervalEnd: string;
  dataIntervalStart: string;
  notes: string;
  runId: string;
};

export const useTriggerDag = () => {
  const { error, mutate: triggerDagRun } = useDagRunServiceTriggerDagRun();

  const triggerDag = (dagParams: DagParams) => {
    const {
      configJson,
      dagId,
      dataIntervalEnd,
      dataIntervalStart,
      notes,
      runId,
    } = dagParams;

    try {
      // Parse the configJson string into a JavaScript object
      const parsedConfig = JSON.parse(configJson) as Record<string, unknown>;

      // Validate and format data intervals
      const formattedDataIntervalStart = dataIntervalStart
        ? new Date(dataIntervalStart).toISOString()
        : undefined; // Use null if empty or invalid
      const formattedDataIntervalEnd = dataIntervalEnd
        ? new Date(dataIntervalEnd).toISOString()
        : undefined; // Use null if empty or invalid

      // Prepare the request body for the API call
      const requestBody: TriggerDAGRunPostBody = {
        conf: parsedConfig,
        dag_run_id: runId,
        data_interval_end: formattedDataIntervalEnd,
        data_interval_start: formattedDataIntervalStart,
        note: notes,
      };

      // Trigger the DAG run
      triggerDagRun({
        dagId,
        requestBody,
      });
    } catch (_error) {
      // TODO
      console.error("Failed to parse configJson or trigger DAG run:", _error);
    }
  };

  return { error, triggerDag };
};
