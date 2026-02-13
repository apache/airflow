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
import { Box, Flex } from "@chakra-ui/react";

import type { TaskInstanceState } from "openapi/requests/types.gen";
import { sortStateEntries } from "src/utils";

type Props = {
  readonly childStates: Record<string, number> | null;
  readonly fallbackState?: TaskInstanceState | null;
  readonly height?: number | string;
};

export const SegmentedStateBar = ({ childStates, fallbackState, height = "6px" }: Props) => {
  const entries = sortStateEntries(childStates);

  if (entries.length === 0) {
    if (!fallbackState) {
      return undefined;
    }

    return <Box bg={`${fallbackState}.solid`} borderRadius="2px" height={height} mt="auto" />;
  }

  return (
    <Flex borderRadius="2px" height={height} mt="auto" overflow="hidden">
      {entries.map(([state, count]) => (
        <Box bg={`${state}.solid`} flex={count} key={state} minWidth="2px" />
      ))}
    </Flex>
  );
};
