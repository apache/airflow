// generated with @7nohe/openapi-react-query-codegen@1.6.0
import { UseQueryResult } from "@tanstack/react-query";

import { AssetService, DagService } from "../requests/services.gen";
import { DagRunState } from "../requests/types.gen";

export type AssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetDefaultResponse =
  Awaited<
    ReturnType<typeof AssetService.nextRunAssetsUiNextRunDatasetsDagIdGet>
  >;
export type AssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetQueryResult<
  TData = AssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useAssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetKey =
  "AssetServiceNextRunAssetsUiNextRunDatasetsDagIdGet";
export const UseAssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetKeyFn = (
  {
    dagId,
  }: {
    dagId: string;
  },
  queryKey?: Array<unknown>,
) => [
  useAssetServiceNextRunAssetsUiNextRunDatasetsDagIdGetKey,
  ...(queryKey ?? [{ dagId }]),
];
export type DagServiceGetDagsPublicDagsGetDefaultResponse = Awaited<
  ReturnType<typeof DagService.getDagsPublicDagsGet>
>;
export type DagServiceGetDagsPublicDagsGetQueryResult<
  TData = DagServiceGetDagsPublicDagsGetDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useDagServiceGetDagsPublicDagsGetKey =
  "DagServiceGetDagsPublicDagsGet";
export const UseDagServiceGetDagsPublicDagsGetKeyFn = (
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
  queryKey?: Array<unknown>,
) => [
  useDagServiceGetDagsPublicDagsGetKey,
  ...(queryKey ?? [
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
    },
  ]),
];
export type DagServicePatchDagsPublicDagsPatchMutationResult = Awaited<
  ReturnType<typeof DagService.patchDagsPublicDagsPatch>
>;
export type DagServicePatchDagPublicDagsDagIdPatchMutationResult = Awaited<
  ReturnType<typeof DagService.patchDagPublicDagsDagIdPatch>
>;
