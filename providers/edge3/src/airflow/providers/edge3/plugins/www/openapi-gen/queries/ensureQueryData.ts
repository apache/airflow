// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
import { LogsService, MonitorService, UiService } from "../requests/services.gen";
import { EdgeWorkerState } from "../requests/types.gen";
import * as Common from "./common";
export const ensureUseLogsServiceLogfilePathData = (queryClient: QueryClient, { authorization, dagId, mapIndex, runId, taskId, tryNumber }: {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  taskId: string;
  tryNumber: number;
}) => queryClient.ensureQueryData({ queryKey: Common.UseLogsServiceLogfilePathKeyFn({ authorization, dagId, mapIndex, runId, taskId, tryNumber }), queryFn: () => LogsService.logfilePath({ authorization, dagId, mapIndex, runId, taskId, tryNumber }) });
export const ensureUseMonitorServiceHealthData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseMonitorServiceHealthKeyFn(), queryFn: () => MonitorService.health() });
export const ensureUseUiServiceWorkerData = (queryClient: QueryClient, { queueNamePattern, state, workerNamePattern }: {
  queueNamePattern?: string;
  state?: EdgeWorkerState[];
  workerNamePattern?: string;
} = {}) => queryClient.ensureQueryData({ queryKey: Common.UseUiServiceWorkerKeyFn({ queueNamePattern, state, workerNamePattern }), queryFn: () => UiService.worker({ queueNamePattern, state, workerNamePattern }) });
export const ensureUseUiServiceJobsData = (queryClient: QueryClient) => queryClient.ensureQueryData({ queryKey: Common.UseUiServiceJobsKeyFn(), queryFn: () => UiService.jobs() });
