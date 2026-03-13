// generated with @7nohe/openapi-react-query-codegen@2.0.0
import { type Options } from "@hey-api/client-axios";
import { UseQueryResult } from "@tanstack/react-query";

import { createToken, createTokenAllAdmins, createTokenCli, loginAllAdmins } from "../requests/services.gen";

export type CreateTokenAllAdminsDefaultResponse = Awaited<ReturnType<typeof createTokenAllAdmins>>["data"];
export type CreateTokenAllAdminsQueryResult<
  TData = CreateTokenAllAdminsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useCreateTokenAllAdminsKey = "CreateTokenAllAdmins";
export const UseCreateTokenAllAdminsKeyFn = (
  clientOptions: Options<unknown, true> = {},
  queryKey?: Array<unknown>,
) => [useCreateTokenAllAdminsKey, ...(queryKey ?? [clientOptions])];
export type LoginAllAdminsDefaultResponse = Awaited<ReturnType<typeof loginAllAdmins>>["data"];
export type LoginAllAdminsQueryResult<
  TData = LoginAllAdminsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useLoginAllAdminsKey = "LoginAllAdmins";
export const UseLoginAllAdminsKeyFn = (
  clientOptions: Options<unknown, true> = {},
  queryKey?: Array<unknown>,
) => [useLoginAllAdminsKey, ...(queryKey ?? [clientOptions])];
export type CreateTokenMutationResult = Awaited<ReturnType<typeof createToken>>;
export const useCreateTokenKey = "CreateToken";
export const UseCreateTokenKeyFn = (mutationKey?: Array<unknown>) => [
  useCreateTokenKey,
  ...(mutationKey ?? []),
];
export type CreateTokenCliMutationResult = Awaited<ReturnType<typeof createTokenCli>>;
export const useCreateTokenCliKey = "CreateTokenCli";
export const UseCreateTokenCliKeyFn = (mutationKey?: Array<unknown>) => [
  useCreateTokenCliKey,
  ...(mutationKey ?? []),
];
