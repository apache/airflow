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
import { Alert, AlertIcon } from "@chakra-ui/react";
import type { ApiError } from "openapi-gen/requests/core/ApiError";
import type {
  HTTPExceptionResponse,
  HTTPValidationError,
} from "openapi-gen/requests/types.gen";

type ExpandedApiError = {
  body: HTTPExceptionResponse | HTTPValidationError;
} & ApiError;

type Props = {
  readonly error?: unknown;
};

export const ErrorAlert = ({ error: err }: Props) => {
  const error = err as ExpandedApiError;

  if (!Boolean(error)) {
    return undefined;
  }

  const details = error.body.detail;
  let detailMessage;

  if (details !== undefined) {
    if (typeof details === "string") {
      detailMessage = details;
    } else if (Array.isArray(details)) {
      detailMessage = details.map(
        (detail) => `
          ${detail.loc.join(".")} ${detail.msg}`,
      );
    } else {
      detailMessage = Object.keys(details).map(
        (key) => `${key}: ${details[key] as string}`,
      );
    }
  }

  return (
    <Alert status="error">
      <AlertIcon />
      {error.message}
      <br />
      {detailMessage}
    </Alert>
  );
};
