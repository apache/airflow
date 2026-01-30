// generated with @7nohe/openapi-react-query-codegen@2.0.0
import { type Options } from "@hey-api/client-axios";
import { useMutation, UseMutationOptions, useQuery, UseQueryOptions } from "@tanstack/react-query";
import { AxiosError } from "axios";

import { createToken, createTokenAllAdmins, createTokenCli, loginAllAdmins } from "../requests/services.gen";
import {
  CreateTokenAllAdminsError,
  CreateTokenCliData,
  CreateTokenCliError,
  CreateTokenData,
  CreateTokenError,
} from "../requests/types.gen";
import * as Common from "./common";

export const useCreateTokenAllAdmins = <
  TData = Common.CreateTokenAllAdminsDefaultResponse,
  TError = AxiosError<CreateTokenAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<unknown, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () =>
      createTokenAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
export const useLoginAllAdmins = <
  TData = Common.LoginAllAdminsDefaultResponse,
  TError = AxiosError<LoginAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<unknown, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () => loginAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
export const useCreateToken = <
  TData = Common.CreateTokenMutationResult,
  TError = AxiosError<CreateTokenError>,
  TQueryKey extends Array<unknown> = unknown[],
  TContext = unknown,
>(
  mutationKey?: TQueryKey,
  options?: Omit<
    UseMutationOptions<TData, TError, Options<CreateTokenData, true>, TContext>,
    "mutationKey" | "mutationFn"
  >,
) =>
  useMutation<TData, TError, Options<CreateTokenData, true>, TContext>({
    mutationKey: Common.UseCreateTokenKeyFn(mutationKey),
    mutationFn: (clientOptions) => createToken(clientOptions) as unknown as Promise<TData>,
    ...options,
  });
export const useCreateTokenCli = <
  TData = Common.CreateTokenCliMutationResult,
  TError = AxiosError<CreateTokenCliError>,
  TQueryKey extends Array<unknown> = unknown[],
  TContext = unknown,
>(
  mutationKey?: TQueryKey,
  options?: Omit<
    UseMutationOptions<TData, TError, Options<CreateTokenCliData, true>, TContext>,
    "mutationKey" | "mutationFn"
  >,
) =>
  useMutation<TData, TError, Options<CreateTokenCliData, true>, TContext>({
    mutationKey: Common.UseCreateTokenCliKeyFn(mutationKey),
    mutationFn: (clientOptions) => createTokenCli(clientOptions) as unknown as Promise<TData>,
    ...options,
  });
