// generated with @7nohe/openapi-react-query-codegen@1.6.2 

import { useMutation, UseMutationOptions, useQuery, UseQueryOptions } from "@tanstack/react-query";
import { SimpleAuthManagerLoginService } from "../requests/services.gen";
import { LoginBody } from "../requests/types.gen";
import * as Common from "./common";
/**
* Create Token All Admins
* Create a token with no credentials only if ``simple_auth_manager_all_admins`` is True.
* @returns LoginResponse Successful Response
* @throws ApiError
*/
export const useSimpleAuthManagerLoginServiceCreateTokenAllAdmins = <TData = Common.SimpleAuthManagerLoginServiceCreateTokenAllAdminsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseSimpleAuthManagerLoginServiceCreateTokenAllAdminsKeyFn(queryKey), queryFn: () => SimpleAuthManagerLoginService.createTokenAllAdmins() as TData, ...options });
/**
* Login All Admins
* Login the user with no credentials.
* @throws ApiError
*/
export const useSimpleAuthManagerLoginServiceLoginAllAdmins = <TData = Common.SimpleAuthManagerLoginServiceLoginAllAdminsDefaultResponse, TError = unknown, TQueryKey extends Array<unknown> = unknown[]>(queryKey?: TQueryKey, options?: Omit<UseQueryOptions<TData, TError>, "queryKey" | "queryFn">) => useQuery<TData, TError>({ queryKey: Common.UseSimpleAuthManagerLoginServiceLoginAllAdminsKeyFn(queryKey), queryFn: () => SimpleAuthManagerLoginService.loginAllAdmins() as TData, ...options });
/**
* Create Token
* Authenticate the user.
* @param data The data for the request.
* @param data.requestBody
* @param data.contentType Content-Type of the request body
* @returns LoginResponse Successful Response
* @throws ApiError
*/
export const useSimpleAuthManagerLoginServiceCreateToken = <TData = Common.SimpleAuthManagerLoginServiceCreateTokenMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  contentType?: "application/json" | "application/x-www-form-urlencoded";
  requestBody: LoginBody;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  contentType?: "application/json" | "application/x-www-form-urlencoded";
  requestBody: LoginBody;
}, TContext>({ mutationFn: ({ contentType, requestBody }) => SimpleAuthManagerLoginService.createToken({ contentType, requestBody }) as unknown as Promise<TData>, ...options });
/**
* Create Token Cli
* Authenticate the user for the CLI.
* @param data The data for the request.
* @param data.requestBody
* @returns LoginResponse Successful Response
* @throws ApiError
*/
export const useSimpleAuthManagerLoginServiceCreateTokenCli = <TData = Common.SimpleAuthManagerLoginServiceCreateTokenCliMutationResult, TError = unknown, TContext = unknown>(options?: Omit<UseMutationOptions<TData, TError, {
  requestBody: LoginBody;
}, TContext>, "mutationFn">) => useMutation<TData, TError, {
  requestBody: LoginBody;
}, TContext>({ mutationFn: ({ requestBody }) => SimpleAuthManagerLoginService.createTokenCli({ requestBody }) as unknown as Promise<TData>, ...options });
