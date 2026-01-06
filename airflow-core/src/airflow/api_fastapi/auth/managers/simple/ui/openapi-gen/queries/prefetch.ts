// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
import { SimpleAuthManagerLoginService } from "../requests/services.gen";
import * as Common from "./common";
/**
* Create Token All Admins
* Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
* @returns LoginResponse Successful Response
* @throws ApiError
*/
export const prefetchUseSimpleAuthManagerLoginServiceCreateTokenAllAdmins = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseSimpleAuthManagerLoginServiceCreateTokenAllAdminsKeyFn(), queryFn: () => SimpleAuthManagerLoginService.createTokenAllAdmins() });
/**
* Login All Admins
* Login the user with no credentials.
* @throws ApiError
*/
export const prefetchUseSimpleAuthManagerLoginServiceLoginAllAdmins = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseSimpleAuthManagerLoginServiceLoginAllAdminsKeyFn(), queryFn: () => SimpleAuthManagerLoginService.loginAllAdmins() });
