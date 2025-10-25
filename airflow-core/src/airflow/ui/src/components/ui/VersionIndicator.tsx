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
    color="orange.fg"
    left={-2}
    position="absolute"
    title={`Bundle Version: ${bundleVersion}`}
    top={93}
    zIndex={1}
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
      height={isVertical ? 104 : 0.5}
      left={isVertical ? -1.2 : 0}
      position="absolute"
      top={isVertical ? -1.5 : -0.5}
      width={isVertical ? 0.5 : 4.5}
      zIndex={1}
    >
      {isVertical ? (
        <>
          <Box bg="orange.focusRing" height="full" position="absolute" width={0.5} />

          <Box
            _hover={{ "& > .version-tooltip": { opacity: 1, visibility: "visible" } }}
            left="50%"
            position="absolute"
            top={-2}
            transform="translateX(-50%)"
            zIndex="tooltip"
          >
            <Box
              _hover={{
                bg: "orange.emphasis",
                cursor: "pointer",
              }}
              bg="orange.focusRing"
              borderRadius="full"
              height={2}
              transition="all 0.2s"
              width={2}
            />
            <Box
              bg="orange.focusRing"
              borderRadius="full"
              className="version-tooltip"
              left="50%"
              opacity={0}
              pointerEvents="none"
              position="absolute"
              px={0.5}
              py={0.5}
              top={-0.5}
              transform="translateX(-50%)"
              transition="all 0.2s"
              visibility="hidden"
            >
              <Text
                color={{ _dark: "black", _light: "white" }}
                fontSize="2xs"
                fontWeight="bold"
                lineHeight="1"
                whiteSpace="nowrap"
              >
                {`v${dagVersionNumber ?? ""}`}
              </Text>
            </Box>
          </Box>
        </>
      ) : (
        <Box
          _hover={{ "& > .version-tooltip": { opacity: 1, visibility: "visible" } }}
          left="50%"
          position="absolute"
          top={-1.5}
          transform="translateX(-50%)"
          zIndex="tooltip"
        >
          <Box
            _hover={{
              cursor: "pointer",
            }}
            alignItems="center"
            color="orange.fg"
            display="flex"
            justifyContent="center"
            transition="all 0.2s"
          >
            <FiGitCommit size="15px" />
          </Box>
          <Box
            bg="orange.focusRing"
            borderRadius="full"
            className="version-tooltip"
            left="50%"
            opacity={0}
            pointerEvents="none"
            position="absolute"
            px={0.5}
            py={0.5}
            top={0.5}
            transform="translateX(-50%)"
            transition="all 0.2s"
            visibility="hidden"
          >
            <Text
              color={{ _dark: "black", _light: "white" }}
              fontSize="2xs"
              fontWeight="bold"
              lineHeight="1"
              whiteSpace="nowrap"
            >
              {`v${dagVersionNumber ?? ""}`}
            </Text>
          </Box>
        </Box>
      )}
    </Box>
  );
};
