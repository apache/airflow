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
import { Text, VStack } from "@chakra-ui/react";

import { Alert } from "src/components/ui";

import type { BulkErrorEntry } from "./bulkActionTypes";

type Props = {
  readonly errors: ReadonlyArray<BulkErrorEntry>;
};

const formatRunRef = (entry: BulkErrorEntry): string => {
  const parts = [entry.dag_id, entry.dag_run_id].filter(
    (part): part is string => part !== undefined && part !== null,
  );

  return parts.length > 0 ? parts.join(" / ") : "(unknown run)";
};

export const BulkErrorList = ({ errors }: Props) => {
  if (errors.length === 0) {
    return undefined;
  }

  return (
    <Alert data-testid="bulk-error-list" status="error">
      <VStack align="start" gap={1}>
        {errors.map((entry) => (
          <Text fontSize="sm" key={`${formatRunRef(entry)}:${entry.error}`}>
            <Text as="span" fontWeight="bold">
              {formatRunRef(entry)}
            </Text>
            : {entry.error}
          </Text>
        ))}
      </VStack>
    </Alert>
  );
};
