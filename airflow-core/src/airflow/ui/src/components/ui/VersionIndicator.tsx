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
import { Box, Text } from "@chakra-ui/react";

type VersionIndicatorProps = {
  readonly orientation: "horizontal" | "vertical";
  readonly styleOverrides?: object;
  readonly versionChanges?: Array<number>;
  readonly versionNumber?: number | null;
};

export const VersionIndicator = ({
  orientation,
  styleOverrides,
  versionChanges,
  versionNumber,
}: VersionIndicatorProps) => {
  const isVertical = orientation === "vertical";

  return (
    <Box
      bg="orange.400"
      height={isVertical ? "100px" : "2px"}
      left={isVertical ? "-1px" : "0"}
      position="absolute"
      top={isVertical ? "0" : "-1px"}
      width={isVertical ? "2px" : "18px"}
      zIndex={3}
      {...styleOverrides}
    >
      <Text
        bg="white"
        borderRadius="2px"
        color="orange.700"
        fontSize={isVertical ? "10px" : "8px"}
        left={isVertical ? "-5px" : undefined}
        position="absolute"
        px="1px"
        right={isVertical ? undefined : "-8px"}
        title={
          versionChanges && versionChanges.length > 0
            ? `New versions: ${versionChanges.join(", ")}`
            : undefined
        }
        top={isVertical ? "-16px" : "-4px"}
        whiteSpace="nowrap"
      >
        {`v${versionNumber ?? ""}`}
      </Text>
    </Box>
  );
};
