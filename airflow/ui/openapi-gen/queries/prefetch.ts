// generated with @7nohe/openapi-react-query-codegen@1.6.0 

import { type QueryClient } from "@tanstack/react-query";
import { DagService, DatasetService } from "../requests/services.gen";
import * as Common from "./common";
/**
* Next Run Datasets
* @param data The data for the request.
* @param data.dagId
* @returns unknown Successful Response
* @throws ApiError
*/
export const prefetchUseDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGet = (queryClient: QueryClient, { dagId }: {
  dagId: string;
}) => queryClient.prefetchQuery({ queryKey: Common.UseDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetKeyFn({ dagId }), queryFn: () => DatasetService.nextRunDatasetsUiNextRunDatasetsDagIdGet({ dagId }) });
/**
* Get Dags
* Get all DAGs.
* @param data The data for the request.
* @param data.limit
* @param data.offset
* @param data.tags
* @param data.dagIdPattern
* @param data.onlyActive
* @param data.paused
* @param data.orderBy
* @returns DAGCollectionResponse Successful Response
* @throws ApiError
*/
export const prefetchUseDagServiceGetDagsPublicDagsGet = (queryClient: QueryClient, { dagIdPattern, limit, offset, onlyActive, orderBy, paused, tags }: {
  dagIdPattern?: string;
  limit?: number;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string;
  paused?: boolean;
  tags?: string[];
} = {}) => queryClient.prefetchQuery({ queryKey: Common.UseDagServiceGetDagsPublicDagsGetKeyFn({ dagIdPattern, limit, offset, onlyActive, orderBy, paused, tags }), queryFn: () => DagService.getDagsPublicDagsGet({ dagIdPattern, limit, offset, onlyActive, orderBy, paused, tags }) });
