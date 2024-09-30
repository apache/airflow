// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { type QueryClient } from "@tanstack/react-query";

import { DagService, DatasetService } from "../requests/services.gen";
import { DagRunState } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Next Run Datasets
 * @param data The data for the request.
 * @param data.dagId
 * @returns unknown Successful Response
 * @throws ApiError
 */
export const prefetchUseDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGet =
  (
    queryClient: QueryClient,
    {
      dagId,
    }: {
      dagId: string;
    },
  ) =>
    queryClient.prefetchQuery({
      queryKey:
        Common.UseDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetKeyFn({
          dagId,
        }),
      queryFn: () =>
        DatasetService.nextRunDatasetsUiNextRunDatasetsDagIdGet({ dagId }),
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
export const prefetchUseDagServiceGetDagsPublicDagsGet = (
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
    queryKey: Common.UseDagServiceGetDagsPublicDagsGetKeyFn({
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
      DagService.getDagsPublicDagsGet({
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
