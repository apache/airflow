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
import { useTranslation } from "react-i18next";
import { FiGitCommit } from "react-icons/fi";

import { Tooltip } from "src/components/ui";

import {
  BUNDLE_VERSION_ICON_SIZE,
  BUNDLE_VERSION_INDICATOR_LEFT,
  BUNDLE_VERSION_INDICATOR_TOP,
  DAG_VERSION_INDICATOR_HEIGHT,
  VERSION_INDICATOR_Z_INDEX,
} from "./constants";

type BundleVersionIndicatorProps = {
  readonly bundleVersion: string | null | undefined;
};

export const BundleVersionIndicator = ({ bundleVersion }: BundleVersionIndicatorProps) => {
  const { t: translate } = useTranslation("components");

  return (
    <Tooltip content={`${translate("versionDetails.bundleVersion")}: ${bundleVersion}`}>
      <Box
        color="orange.focusRing"
        left={BUNDLE_VERSION_INDICATOR_LEFT}
        position="absolute"
        top={BUNDLE_VERSION_INDICATOR_TOP}
        zIndex={VERSION_INDICATOR_Z_INDEX}
      >
        <FiGitCommit size={BUNDLE_VERSION_ICON_SIZE} />
      </Box>
    </Tooltip>
  );
};

const CONTAINER_STYLES = {
  horizontal: {
    height: 0.5,
    left: "50%",
    top: 0,
    transform: "translate(-50%, -50%)",
    width: 4.5,
  },
  vertical: {
    height: DAG_VERSION_INDICATOR_HEIGHT,
    left: -1.25,
    top: -1.5,
    width: 0.5,
  },
} as const;

const CIRCLE_STYLES = {
  horizontal: {
    height: 1.5,
    left: "50%",
    top: "50%",
    transform: "translate(-50%, -50%)",
    width: 1.5,
  },
  vertical: {
    height: 1.5,
    left: "50%",
    top: -1,
    transform: "translateX(-50%)",
    width: 1.5,
  },
} as const;

type DagVersionIndicatorProps = {
  readonly dagVersionNumber: number | undefined;
  readonly orientation?: "horizontal" | "vertical";
};

export const DagVersionIndicator = ({
  dagVersionNumber,
  orientation = "vertical",
}: DagVersionIndicatorProps) => {
  const isVertical = orientation === "vertical";
  const currentContainerStyle = CONTAINER_STYLES[orientation];
  const currentCircleStyle = CIRCLE_STYLES[orientation];

  return (
    <Box
      aria-label={`Version ${dagVersionNumber} indicator`}
      as="output"
      position="absolute"
      zIndex={VERSION_INDICATOR_Z_INDEX}
      {...currentContainerStyle}
    >
      <Box
        bg="orange.focusRing"
        height={isVertical ? "full" : 0.5}
        position="absolute"
        width={isVertical ? 0.5 : "full"}
      />

      <Tooltip
        closeDelay={0}
        content={`v${dagVersionNumber ?? ""}`}
        openDelay={0}
        portalled
        positioning={{
          placement: isVertical ? "top" : "right",
        }}
      >
        <Box
          _hover={{
            cursor: "pointer",
            transform: `${currentCircleStyle.transform} scale(1.2)`,
          }}
          bg="orange.focusRing"
          borderRadius="full"
          position="absolute"
          transition="all 0.2s ease-in-out"
          zIndex="tooltip"
          {...currentCircleStyle}
        />
      </Tooltip>
    </Box>
  );
};
