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
import { Box, Code, Skeleton, VStack } from "@chakra-ui/react";
import type { ReactNode } from "react";

import { ErrorAlert } from "src/components/ErrorAlert";
import { ProgressBar } from "src/components/ui";

type Props = {
  readonly error: unknown;
  readonly isLoading: boolean;
  readonly logError: unknown;
  readonly parsedLogs: ReactNode;
  readonly wrap: boolean;
};

export const TaskLogContent = ({ error, isLoading, logError, parsedLogs, wrap }: Props) => (
  <Box>
    <ErrorAlert error={error ?? logError} />
    <Skeleton />
    <ProgressBar size="xs" visibility={isLoading ? "visible" : "hidden"} />
    <Code overflow="auto" py={3} textWrap={wrap ? "pre" : "nowrap"} width="100%">
      <VStack alignItems="flex-start">{parsedLogs}</VStack>
    </Code>
  </Box>
);
