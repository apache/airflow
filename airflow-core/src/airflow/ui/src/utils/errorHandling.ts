/*!
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import type { TFunction } from "i18next";
import type { HTTPExceptionResponse, HTTPValidationError } from "openapi-gen/requests/types.gen";

import { toaster } from "src/system-components";

/**
 * Type guard to check if an error has a status property
 */
type ErrorWithStatus = {
  body?: HTTPExceptionResponse | HTTPValidationError;
  message?: string;
  response?: {
    status?: number;
  };
  status?: number;
};

/**
 * Safely extracts the HTTP status code from an error object
 */
export const getErrorStatus = (error: unknown): number | undefined => {
  if (typeof error !== "object" || error === null) {
    return undefined;
  }

  const errorObj = error as ErrorWithStatus;

  return errorObj.status ?? errorObj.response?.status;
};

export const getErrorDetail = (error: unknown): string | undefined => {
  if (typeof error !== "object" || error === null) {
    return undefined;
  }

  const detail = (error as ErrorWithStatus).body?.detail;

  if (detail === undefined) {
    return undefined;
  }

  if (typeof detail === "string") {
    return detail;
  }

  if (Array.isArray(detail)) {
    return detail.map((item) => `${item.loc.join(".")} ${item.msg}`).join("\n");
  }

  return Object.keys(detail)
    .map((key) => `${key}: ${detail[key] as string}`)
    .join("\n");
};

/**
 * Safely extracts the error message from an error object
 */
const getErrorMessage = (error: unknown): string => {
  if (typeof error !== "object" || error === null) {
    return String(error);
  }

  const errorObj = error as ErrorWithStatus;

  return getErrorDetail(error) ?? errorObj.message ?? "An error occurred";
};

/**
 * Creates an error toaster notification with standardized behavior.
 * Skips 403 errors as they are handled by MutationCache.
 *
 * @param error - The error object to process
 * @param options - Configuration options
 * @param options.params - Optional parameters for translation interpolation
 * @param options.titleKey - The translation key for the error title
 * @param translate - The i18next translate function
 */
export const createErrorToaster = (
  error: unknown,
  options: { params?: Record<string, string>; titleKey: string },
  translate: TFunction,
): void => {
  const status = getErrorStatus(error);

  // Skip 403 errors as they are handled by MutationCache
  if (status === 403) {
    return;
  }

  const message = getErrorMessage(error);
  const title = translate(options.titleKey, options.params);

  toaster.create({
    description: message,
    title,
    type: "error",
  });
};
