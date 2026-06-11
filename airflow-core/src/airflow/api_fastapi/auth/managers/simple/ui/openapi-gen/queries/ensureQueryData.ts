// generated with @7nohe/openapi-react-query-codegen@2.1.0
import { type QueryClient } from "@tanstack/react-query";

import type { Options } from "../requests/sdk.gen";
import { createTokenAllAdmins, loginAllAdmins } from "../requests/sdk.gen";
import { CreateTokenAllAdminsData, LoginAllAdminsData } from "../requests/types.gen";
import * as Common from "./common";

/**
 * Create Token All Admins
 *
 * Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
 */
export const ensureUseCreateTokenAllAdminsData = (
  queryClient: QueryClient,
  clientOptions: Options<CreateTokenAllAdminsData, true> = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions),
    queryFn: () => createTokenAllAdmins({ ...clientOptions }).then((response) => response.data),
  });
/**
 * Login All Admins
 *
 * Login the user with no credentials.
 */
export const ensureUseLoginAllAdminsData = (
  queryClient: QueryClient,
  clientOptions: Options<LoginAllAdminsData, true> = {},
) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions),
    queryFn: () => loginAllAdmins({ ...clientOptions }).then((response) => response.data),
  });
