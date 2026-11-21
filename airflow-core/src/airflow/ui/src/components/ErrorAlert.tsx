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
import { useState } from "react";

import { HStack, Text } from "@chakra-ui/react";
import type { ApiError } from "openapi-gen/requests/core/ApiError";
import type { HTTPExceptionResponse, HTTPValidationError } from "openapi-gen/requests/types.gen";

import { Alert } from "src/system-components";

import { getApiErrorDetail } from "src/utils";

export type ExpandedApiError = {
  body: HTTPExceptionResponse | HTTPValidationError | undefined;
} & ApiError;

type Props = {
  /** Offer a close button. The alert returns on the next, different error. */
  readonly closable?: boolean;
  readonly error?: unknown;
};

export const ErrorAlert = ({ closable, error: err }: Props) => {
  const [dismissedError, setDismissedError] = useState<unknown>(undefined);

  // A fresh failure is new information, so it shows even where the last one was dismissed.
  // React Query hands back a new error object per failed fetch, so identity is the signal.
  const isDismissed = dismissedError !== undefined && dismissedError === err;

  const error = err as ExpandedApiError;

  if (!Boolean(error) || isDismissed) {
    return undefined;
  }

  const detailMessage = getApiErrorDetail(error);

  return (
    <Alert
      closable={closable}
      data-testid="error-alert"
      onClose={() => setDismissedError(err)}
      status="error"
    >
      <HStack align="start" flexDirection="column" gap={2} mt={-1}>
        {error.status} {error.message}
        {detailMessage === error.message ? undefined : (
          <Text whiteSpace="preserve" wordBreak="break-all">
            {detailMessage}
          </Text>
        )}
      </HStack>
    </Alert>
  );
};
