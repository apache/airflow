// generated with @7nohe/openapi-react-query-codegen@1.6.2
import { type QueryClient } from "@tanstack/react-query";

import { SimpleAuthManagerLoginService } from "../requests/services.gen";
import * as Common from "./common";

export const ensureUseSimpleAuthManagerLoginServiceCreateTokenAllAdminsData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseSimpleAuthManagerLoginServiceCreateTokenAllAdminsKeyFn(),
    queryFn: () => SimpleAuthManagerLoginService.createTokenAllAdmins(),
  });
export const ensureUseSimpleAuthManagerLoginServiceLoginAllAdminsData = (queryClient: QueryClient) =>
  queryClient.ensureQueryData({
    queryKey: Common.UseSimpleAuthManagerLoginServiceLoginAllAdminsKeyFn(),
    queryFn: () => SimpleAuthManagerLoginService.loginAllAdmins(),
  });
