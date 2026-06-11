// generated with @7nohe/openapi-react-query-codegen@2.1.0
import { useMutation, UseMutationOptions, useQuery, UseQueryOptions } from "@tanstack/react-query";
import { AxiosError } from "axios";

import type { Options } from "../requests/sdk.gen";
import { createToken, createTokenAllAdmins, createTokenCli, loginAllAdmins } from "../requests/sdk.gen";
import {
  CreateTokenAllAdminsData,
  CreateTokenAllAdminsError,
  CreateTokenCliData,
  CreateTokenCliError,
  CreateTokenData,
  CreateTokenError,
  LoginAllAdminsData,
  LoginAllAdminsError,
} from "../requests/types.gen";
import * as Common from "./common";

/**
 * Create Token All Admins
 *
 * Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
 */
export const useCreateTokenAllAdmins = <
  TData = Common.CreateTokenAllAdminsDefaultResponse,
  TError = AxiosError<CreateTokenAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<CreateTokenAllAdminsData, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () =>
      createTokenAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
/**
 * Login All Admins
 *
 * Login the user with no credentials.
 */
export const useLoginAllAdmins = <
  TData = Common.LoginAllAdminsDefaultResponse,
  TError = AxiosError<LoginAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<LoginAllAdminsData, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useQuery<TData, TError>({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () => loginAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
/**
 * Create Token
 *
 * Authenticate the user.
 */
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
/**
 * Create Token Cli
 *
 * Authenticate the user for the CLI.
 */
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
