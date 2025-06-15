// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { type QueryClient } from "@tanstack/react-query";
import { SimpleAuthManagerLoginService } from "../requests/services.gen";
import * as Common from "./common";
export const prefetchUseSimpleAuthManagerLoginServiceCreateTokenAllAdmins = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseSimpleAuthManagerLoginServiceCreateTokenAllAdminsKeyFn(), queryFn: () => SimpleAuthManagerLoginService.createTokenAllAdmins() });
export const prefetchUseSimpleAuthManagerLoginServiceLoginAllAdmins = (queryClient: QueryClient) => queryClient.prefetchQuery({ queryKey: Common.UseSimpleAuthManagerLoginServiceLoginAllAdminsKeyFn(), queryFn: () => SimpleAuthManagerLoginService.loginAllAdmins() });
