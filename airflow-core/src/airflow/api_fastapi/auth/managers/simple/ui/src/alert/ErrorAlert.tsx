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
import { HStack } from "@chakra-ui/react";
import type { AxiosError } from "axios";

import type { HttpExceptionResponse, HttpValidationError } from "openapi/requests/types.gen";

import { Alert } from "./Alert";

type Props = {
  readonly error?: unknown;
};

export const ErrorAlert = ({ error: err }: Props) => {
  if (err === undefined || err === null) {
    return undefined;
  }

  const error = err as AxiosError<HttpExceptionResponse | HttpValidationError>;
  const { message, response } = error;
  const statusCode = response?.status;
  const statusText = response?.statusText ?? message;
  const detail = response?.data.detail;
  let detailMessage: React.ReactNode;

  if (typeof detail === "string") {
    detailMessage = detail;
  } else if (Array.isArray(detail)) {
    detailMessage = detail.map((entry) => `${entry.loc.join(".")} ${entry.msg}`).join(", ");
  }

  return (
    <Alert status="error">
      <HStack align="start" flexDirection="column" gap={2} mt={-1}>
        {statusCode} {statusText}
        {detailMessage !== undefined && detailMessage !== statusText ? (
          <span>{detailMessage}</span>
        ) : undefined}
      </HStack>
    </Alert>
  );
};
