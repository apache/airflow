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
import { FiGitCommit } from "react-icons/fi";

export const BundleVersionIndicator = ({ bundleVersion }: { readonly bundleVersion: string | null }) => (
  <Box
    color="orange.700"
    left="-7px"
    position="absolute"
    title={`Bundle Version: ${bundleVersion}`}
    top="93px"
  >
    <FiGitCommit size="15px" />
  </Box>
);

export const DagVersionIndicator = ({
  dagVersionNumber,
  orientation = "vertical",
}: {
  readonly dagVersionNumber: number | null;
  readonly orientation?: "horizontal" | "vertical";
}) => {
  const isVertical = orientation === "vertical";

  return (
    <Box
      aria-label={`Version ${dagVersionNumber} indicator`}
      as="output"
      height={isVertical ? "100px" : "2px"}
      left={isVertical ? "-1px" : "0"}
      position="absolute"
      top={isVertical ? "0" : "-1px"}
      width={isVertical ? "2px" : "18px"}
      zIndex={3}
    >
      {isVertical ? (
        <>
          <Box bg="orange.400" height="100px" width="2px" />
          <Text
            bg="white"
            borderRadius="2px"
            color="orange.700"
            fontSize="10px"
            left="-5px"
            position="absolute"
            px="1px"
            title={`Version ${dagVersionNumber}`}
            top="-16px"
            whiteSpace="nowrap"
          >
            {`v${dagVersionNumber ?? ""}`}
          </Text>
        </>
      ) : (
        <>
          <Box bg="orange.400" height="2px" left="0" position="absolute" top="0" width="4px" />
          <Box
            alignItems="center"
            bg="transparent"
            borderRadius="2px"
            display="flex"
            height="10px"
            justifyContent="center"
            left="4px"
            position="absolute"
            top="-4px"
            width="10px"
          >
            <Text
              bg="white"
              borderRadius="2px"
              color="orange.700"
              fontSize="6px"
              left="0"
              position="absolute"
              px="1px"
              title={`Version ${dagVersionNumber}`}
              top="0"
              whiteSpace="nowrap"
            >
              {`v${dagVersionNumber ?? ""}`}
            </Text>
          </Box>
          <Box bg="orange.400" height="2px" left="14px" position="absolute" top="0" width="4px" />
        </>
      )}
    </Box>
  );
};
