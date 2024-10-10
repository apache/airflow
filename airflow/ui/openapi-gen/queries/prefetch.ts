// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { type QueryClient } from "@tanstack/react-query";

import {
  AssetService,
  ConnectionService,
  DagRunService,
  DagService,
  DashboardService,
  VariableService,
} from "../requests/services.gen";
import { DagRunState } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Next Run Assets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const prefetchUseAssetServiceNextRunAssets = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseAssetServiceNextRunAssetsKeyFn({ dagId }),
    queryFn: () => AssetService.nextRunAssets({ dagId }),
  });
/**
 * Historical Metrics
 * Return cluster activity historical metrics.
 * @param data The data for the request.
 * @param data.startDate
 * @param data.endDate
 * @returns HistoricalMetricDataResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDashboardServiceHistoricalMetrics = (
  queryClient: QueryClient,
  {
    endDate,
    startDate,
  }: {
    endDate: string;
    startDate: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDashboardServiceHistoricalMetricsKeyFn({
      endDate,
      startDate,
    }),
    queryFn: () => DashboardService.historicalMetrics({ endDate, startDate }),
  });
/**
 * Get Dags
 * Get all DAGs.
 * @param data The data for the request.
 * @param data.limit
 * @param data.offset
 * @param data.tags
 * @param data.owners
 * @param data.dagIdPattern
 * @param data.dagDisplayNamePattern
 * @param data.onlyActive
 * @param data.paused
 * @param data.lastDagRunState
 * @param data.orderBy
 * @returns DAGCollectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDags = (
  queryClient: QueryClient,
  {
    dagDisplayNamePattern,
    dagIdPattern,
    lastDagRunState,
    limit,
    offset,
    onlyActive,
    orderBy,
    owners,
    paused,
    tags,
  }: {
    dagDisplayNamePattern?: string;
    dagIdPattern?: string;
    lastDagRunState?: DagRunState;
    limit?: number;
    offset?: number;
    onlyActive?: boolean;
    orderBy?: string;
    owners?: string[];
    paused?: boolean;
    tags?: string[];
  } = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagsKeyFn({
      dagDisplayNamePattern,
      dagIdPattern,
      lastDagRunState,
      limit,
      offset,
      onlyActive,
      orderBy,
      owners,
      paused,
      tags,
    }),
    queryFn: () =>
      DagService.getDags({
        dagDisplayNamePattern,
        dagIdPattern,
        lastDagRunState,
        limit,
        offset,
        onlyActive,
        orderBy,
        owners,
        paused,
        tags,
      }),
  });
/**
 * Get Dag
 * Get basic information about a DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDag = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagKeyFn({ dagId }),
    queryFn: () => DagService.getDag({ dagId }),
  });
/**
 * Get Dag Details
 * Get details of DAG.
 * @param data The data for the request.
 * @param data.dagId
 * @returns DAGDetailsResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagServiceGetDagDetails = (
  queryClient: QueryClient,
  {
    dagId,
  }: {
    dagId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagServiceGetDagDetailsKeyFn({ dagId }),
    queryFn: () => DagService.getDagDetails({ dagId }),
  });
/**
 * Get Connection
 * Get a connection entry.
 * @param data The data for the request.
 * @param data.connectionId
 * @returns ConnectionResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseConnectionServiceGetConnection = (
  queryClient: QueryClient,
  {
    connectionId,
  }: {
    connectionId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseConnectionServiceGetConnectionKeyFn({ connectionId }),
    queryFn: () => ConnectionService.getConnection({ connectionId }),
  });
/**
 * Get Variable
 * Get a variable entry.
 * @param data The data for the request.
 * @param data.variableKey
 * @returns VariableResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseVariableServiceGetVariable = (
  queryClient: QueryClient,
  {
    variableKey,
  }: {
    variableKey: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseVariableServiceGetVariableKeyFn({ variableKey }),
    queryFn: () => VariableService.getVariable({ variableKey }),
  });
/**
 * Get Dag Run
 * @param data The data for the request.
 * @param data.dagId
 * @param data.dagRunId
 * @returns DAGRunResponse Successful Response
 * @throws ApiError
 */
export const prefetchUseDagRunServiceGetDagRun = (
  queryClient: QueryClient,
  {
    dagId,
    dagRunId,
  }: {
    dagId: string;
    dagRunId: string;
  },
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseDagRunServiceGetDagRunKeyFn({ dagId, dagRunId }),
    queryFn: () => DagRunService.getDagRun({ dagId, dagRunId }),
  });
