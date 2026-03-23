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
import { Box, HStack, Text } from "@chakra-ui/react";

type Props = {
  readonly stateCounts: Record<string, number>;
  readonly total: number;
  readonly trackColor?: string;
};

export const BackfillProgressBar = ({ stateCounts, total, trackColor = "bg.emphasized" }: Props) => {
  const successCount = stateCounts.success ?? 0;
  const failedCount = stateCounts.failed ?? 0;
  const successPct = (successCount / total) * 100;
  const failedPct = (failedCount / total) * 100;
  const remainingPct = 100 - successPct - failedPct;

  return (
    <HStack gap="2" minWidth="60px">
      <HStack flex="1" gap={0}>
        {successPct > 0 ? (
          <Box bg="success.solid" borderLeftRadius={5} height="5px" width={`${successPct}%`} />
        ) : undefined}
        {failedPct > 0 ? (
          <Box
            bg="failed.solid"
            borderLeftRadius={successCount === 0 ? 5 : 0}
            height="5px"
            width={`${failedPct}%`}
          />
        ) : undefined}
        {remainingPct > 0 ? (
          <Box
            bg={trackColor}
            borderLeftRadius={successCount === 0 && failedCount === 0 ? 5 : 0}
            borderRightRadius={5}
            height="5px"
            width={`${remainingPct}%`}
          />
        ) : undefined}
      </HStack>
      <Text fontSize="sm" whiteSpace="nowrap">
        {successCount + failedCount}/{total}
      </Text>
    </HStack>
  );
};
