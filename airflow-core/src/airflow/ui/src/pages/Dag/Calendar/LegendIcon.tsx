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
import { Box } from "@chakra-ui/react";

type LegendColorType =
  | Record<string, string>
  | string
  | { primary: Record<string, string> | string; secondary: Record<string, string> | string };

type Props = {
  readonly color: LegendColorType;
  readonly cursor?: string;
};

export const LegendIcon = ({ color, cursor }: Props) => {
  const isMixedState = typeof color === "object" && "primary" in color && "secondary" in color;

  if (isMixedState) {
    return (
      <Box
        borderRadius="2px"
        boxShadow="sm"
        cursor={cursor}
        height="14px"
        overflow="hidden"
        position="relative"
        width="14px"
      >
        <Box
          bg={color.secondary}
          clipPath="polygon(0 100%, 100% 100%, 0 0)"
          height="100%"
          position="absolute"
          width="100%"
        />
        <Box
          bg={color.primary}
          clipPath="polygon(100% 0, 100% 100%, 0 0)"
          height="100%"
          position="absolute"
          width="100%"
        />
      </Box>
    );
  }

  return <Box bg={color} borderRadius="2px" boxShadow="sm" cursor={cursor} height="14px" width="14px" />;
};
