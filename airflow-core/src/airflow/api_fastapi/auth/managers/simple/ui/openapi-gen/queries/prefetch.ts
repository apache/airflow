// generated with @7nohe/openapi-react-query-codegen@2.0.0
import { type Options } from "@hey-api/client-axios";
import { type QueryClient } from "@tanstack/react-query";

import { createTokenAllAdmins, loginAllAdmins } from "../requests/services.gen";
import * as Common from "./common";

export const prefetchUseCreateTokenAllAdmins = (
  queryClient: QueryClient,
  clientOptions: Options<unknown, true> = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseCreateTokenAllAdminsKeyFn(clientOptions),
    queryFn: () => createTokenAllAdmins({ ...clientOptions }).then((response) => response.data),
  });
export const prefetchUseLoginAllAdmins = (
  queryClient: QueryClient,
  clientOptions: Options<unknown, true> = {},
) =>
  queryClient.prefetchQuery({
    queryKey: Common.UseLoginAllAdminsKeyFn(clientOptions),
    queryFn: () => loginAllAdmins({ ...clientOptions }).then((response) => response.data),
  });
