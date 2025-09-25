// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
import { LogsService, MonitorService, UiService } from "../requests/services.gen";
import * as Common from "./common";
export const prefetchUseLogsServiceLogfilePath = (queryClient: QueryClient, { authorization, dagId, mapIndex, runId, taskId, tryNumber }: {
  authorization: string;
  dagId: string;
  mapIndex: number;
  runId: string;
  taskId: string;
  tryNumber: number;
}) => queryClient.prefetchQuery({ queryKey: Common.UseLogsServiceLogfilePathKeyFn({ authorization, dagId, mapIndex, runId, taskId, tryNumber }), queryFn: () => LogsService.logfilePath({ authorization, dagId, mapIndex, runId, taskId, tryNumber }) });
export const prefetchUseMonitorServiceHealth = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseMonitorServiceHealthKeyFn(), queryFn: () => MonitorService.health() });
export const prefetchUseUiServiceWorker = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseUiServiceWorkerKeyFn(), queryFn: () => UiService.worker() });
export const prefetchUseUiServiceJobs = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseUiServiceJobsKeyFn(), queryFn: () => UiService.jobs() });
