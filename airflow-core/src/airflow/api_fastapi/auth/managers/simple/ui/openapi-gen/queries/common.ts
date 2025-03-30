// generated with @7nohe/openapi-react-query-codegen@1.6.2

import { UseQueryResult } from "@tanstack/react-query";
import { SimpleAuthManagerLoginService } from "../requests/services.gen";
export type SimpleAuthManagerLoginServiceCreateTokenAllAdminsDefaultResponse =
  Awaited<
    ReturnType<typeof SimpleAuthManagerLoginService.createTokenAllAdmins>
  >;
export type SimpleAuthManagerLoginServiceCreateTokenAllAdminsQueryResult<
  TData = SimpleAuthManagerLoginServiceCreateTokenAllAdminsDefaultResponse,
  TError = unknown,
> = UseQueryResult<TData, TError>;
export const useSimpleAuthManagerLoginServiceCreateTokenAllAdminsKey =
  "SimpleAuthManagerLoginServiceCreateTokenAllAdmins";
export const UseSimpleAuthManagerLoginServiceCreateTokenAllAdminsKeyFn = (
  queryKey?: Array<unknown>,
) => [
  useSimpleAuthManagerLoginServiceCreateTokenAllAdminsKey,
  ...(queryKey ?? []),
];
export type SimpleAuthManagerLoginServiceCreateTokenMutationResult = Awaited<
  ReturnType<typeof SimpleAuthManagerLoginService.createToken>
>;
export type SimpleAuthManagerLoginServiceCreateTokenCliMutationResult = Awaited<
  ReturnType<typeof SimpleAuthManagerLoginService.createTokenCli>
>;
