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
import { useQuery } from "@tanstack/react-query";
import axios, { type AxiosError } from "axios";

import { usePluginServiceGetPlugins } from "openapi/queries";
import { OpenAPI } from "openapi/requests/core/OpenAPI";

export type TriageCategory = {
  confidence: number;
  name: string;
};

export type TriageRemediation = {
  doc_links: Array<string>;
  steps: Array<string>;
  title: string;
};

export type TriageSummary = {
  categories: Array<TriageCategory>;
  error: string | null;
  log_available: boolean;
  remediations: Array<TriageRemediation>;
  root_cause_summary: string;
};

export type UseTriageSummaryParams = {
  dagId: string;
  enabled?: boolean;
  mapIndex: number;
  runId: string;
  taskId: string;
};

export const useTriageSummary = ({
  dagId,
  enabled = true,
  mapIndex,
  runId,
  taskId,
}: UseTriageSummaryParams) =>
  useQuery({
    enabled: enabled && Boolean(dagId) && Boolean(runId) && Boolean(taskId),
    queryFn: () =>
      axios
        .get<TriageSummary>(`${OpenAPI.BASE}/triage-panel/summary`, {
          params: {
            dag_id: dagId,
            map_index: mapIndex,
            run_id: runId,
            task_id: taskId,
          },
        })
        .then((response) => response.data)
        .catch((error: unknown) => {
          if (!axios.isAxiosError(error)) {
            return Promise.reject(error);
          }

          const status = error.response?.status;

          if (status === 404 || status === 400) {
            return null;
          }

          if (status !== undefined) {
            (error as { status?: number } & AxiosError).status = status;
          }

          return Promise.reject(error);
        }),
    queryKey: ["triage-summary", dagId, runId, taskId, mapIndex],
    retry: false,
    staleTime: 30_000,
  });

export const useTriagePluginEnabled = () => {
  const { data } = usePluginServiceGetPlugins();

  return data?.plugins.some((plugin) => plugin.name === "dag_triage") ?? false;
};
