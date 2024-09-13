// generated with @7nohe/openapi-react-query-codegen@1.6.0 

import { UseQueryResult } from "@tanstack/react-query";
import { DagService, DatasetService } from "../requests/services.gen";
export type DatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetDefaultResponse = Awaited<ReturnType<typeof DatasetService.nextRunDatasetsUiNextRunDatasetsDagIdGet>>;
export type DatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetQueryResult<TData = DatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;
export const useDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetKey = "DatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGet";
export const UseDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetKeyFn = ({ dagId }: {
  dagId: string;
}, queryKey?: Array<unknown>) => [useDatasetServiceNextRunDatasetsUiNextRunDatasetsDagIdGetKey, ...(queryKey ?? [{ dagId }])];
export type DagServiceGetDagsPublicDagsGetDefaultResponse = Awaited<ReturnType<typeof DagService.getDagsPublicDagsGet>>;
export type DagServiceGetDagsPublicDagsGetQueryResult<TData = DagServiceGetDagsPublicDagsGetDefaultResponse, TError = unknown> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagsPublicDagsGetKey = "DagServiceGetDagsPublicDagsGet";
export const UseDagServiceGetDagsPublicDagsGetKeyFn = ({ dagIdPattern, limit, offset, onlyActive, orderBy, paused, tags }: {
  dagIdPattern?: string;
  limit?: number;
  offset?: number;
  onlyActive?: boolean;
  orderBy?: string;
  paused?: boolean;
  tags?: string[];
} = {}, queryKey?: Array<unknown>) => [useDagServiceGetDagsPublicDagsGetKey, ...(queryKey ?? [{ dagIdPattern, limit, offset, onlyActive, orderBy, paused, tags }])];
