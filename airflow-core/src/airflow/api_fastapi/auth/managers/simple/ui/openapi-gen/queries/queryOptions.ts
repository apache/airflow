// generated with @7nohe/openapi-react-query-codegen@3.0.2
import { queryOptions } from "@tanstack/react-query";

import { createTokenAllAdmins, loginAllAdmins, type Options } from "../requests/sdk.gen";
import type { CreateTokenAllAdminsData, LoginAllAdminsData } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Create Token All Admins
 *
 * Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
 */
export const createTokenAllAdminsOptions = (
  clientOptions: Options<CreateTokenAllAdminsData, true> = {},
  queryKey?: Array<unknown>,
) =>
  queryOptions({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: ({ signal }) =>
      createTokenAllAdmins({ ...clientOptions, signal, throwOnError: true }).then(
        (response) => response.data,
      ),
  });
/**
 * Login All Admins
 *
 * Login the user with no credentials.
 */
export const loginAllAdminsOptions = (
  clientOptions: Options<LoginAllAdminsData, true> = {},
  queryKey?: Array<unknown>,
) =>
  queryOptions({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions, queryKey),
    queryFn: ({ signal }) =>
      loginAllAdmins({ ...clientOptions, signal, throwOnError: true }).then((response) => response.data),
  });
