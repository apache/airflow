// generated with @7nohe/openapi-react-query-codegen@2.0.0
import { type Options } from "@hey-api/client-axios";
import { useSuspenseQuery, UseSuspenseQueryOptions } from "@tanstack/react-query";
import { AxiosError } from "axios";

import { createTokenAllAdmins, loginAllAdmins } from "../requests/services.gen";
import { CreateTokenAllAdminsError } from "../requests/types.gen";
import * as Common from "./common";

export const useCreateTokenAllAdminsSuspense = <
  TData = NonNullable<Common.CreateTokenAllAdminsDefaultResponse>,
  TError = AxiosError<CreateTokenAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<unknown, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () =>
      createTokenAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
export const useLoginAllAdminsSuspense = <
  TData = NonNullable<Common.LoginAllAdminsDefaultResponse>,
  TError = AxiosError<LoginAllAdminsError>,
  TQueryKey extends Array<unknown> = unknown[],
>(
  clientOptions: Options<unknown, true> = {},
  queryKey?: TQueryKey,
  options?: Omit<UseSuspenseQueryOptions<TData, TError>, "queryKey" | "queryFn">,
) =>
  useSuspenseQuery<TData, TError>({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: () => loginAllAdmins({ ...clientOptions }).then((response) => response.data as TData) as TData,
    ...options,
  });
