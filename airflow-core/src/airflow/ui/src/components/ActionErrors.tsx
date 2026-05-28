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
import { Stack } from "@chakra-ui/react";

import type { BulkActionResponse } from "openapi/requests/types.gen";
import { ErrorAlert } from "src/components/ErrorAlert";
import { Alert } from "src/components/ui";

type Props = {
  readonly actionResponse?: BulkActionResponse | null;
  readonly error: unknown;
};

export const ActionErrors = ({ actionResponse, error }: Props) => {
  const actionErrors = (actionResponse?.errors ?? []) as Array<{ error: string; status_code?: number }>;

  return (
    <>
      <ErrorAlert error={error} />
      {actionErrors.length > 0 ? (
        <Stack gap={2} mt={3}>
          {actionErrors.map((actionError, index) => (
            // eslint-disable-next-line react/no-array-index-key -- per-entity errors have no stable id
            <Alert key={index} status="error" title={actionError.error} />
          ))}
        </Stack>
      ) : undefined}
    </>
  );
};
