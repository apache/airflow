// generated with @7nohe/openapi-react-query-codegen@2.1.0
import { useSuspenseQuery, UseSuspenseQueryOptions } from "@tanstack/react-query";
import { AxiosError } from "axios";

import type { Options } from "../requests/sdk.gen";
import { createTokenAllAdmins, loginAllAdmins } from "../requests/sdk.gen";
import {
  CreateTokenAllAdminsData,
  CreateTokenAllAdminsError,
  LoginAllAdminsData,
  LoginAllAdminsError,
} from "../requests/types.gen";
import * as Common from "./common";

/**
 * Create Token All Admins
 *
 * Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
 */
export const useCreateTokenAllAdminsSuspense = <
  TData = NonNullable<Common.CreateTokenAllAdminsDefaultResponse>,
  TError = AxiosError<CreateTokenAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<CreateTokenAllAdminsData, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
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
export const useLoginAllAdminsSuspense = <
  TData = NonNullable<Common.LoginAllAdminsDefaultResponse>,
  TError = AxiosError<LoginAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<LoginAllAdminsData, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () => loginAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
