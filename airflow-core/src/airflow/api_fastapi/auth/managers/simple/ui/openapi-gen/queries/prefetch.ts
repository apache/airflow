// generated with @7nohe/openapi-react-query-codegen@3.0.2
import { type FetchQueryOptions, type QueryClient } from "@tanstack/react-query";

import { createTokenAllAdmins, loginAllAdmins, type Options } from "../requests/sdk.gen";
import type { CreateTokenAllAdminsData, LoginAllAdminsData } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Create Token All Admins
 *
 * Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
 */
export const prefetchUseCreateTokenAllAdmins = (
  queryClient: QueryClient,
  clientOptions: Options<CreateTokenAllAdminsData, true> = {},
  options?: Omit<FetchQueryOptions<Common.CreateTokenAllAdminsDefaultResponse>, "queryKey" | "queryFn">,
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions),
    queryFn: ({ signal }) =>
      createTokenAllAdmins({ ...clientOptions, signal, throwOnError: true }).then(
        (response) => response.data,
      ),
    ...options,
  });
/**
 * Login All Admins
 *
 * Login the user with no credentials.
 */
export const prefetchUseLoginAllAdmins = (
  queryClient: QueryClient,
  clientOptions: Options<LoginAllAdminsData, true> = {},
  options?: Omit<FetchQueryOptions<Common.LoginAllAdminsDefaultResponse>, "queryKey" | "queryFn">,
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions),
    queryFn: ({ signal }) =>
      loginAllAdmins({ ...clientOptions, signal, throwOnError: true }).then((response) => response.data),
    ...options,
  });
