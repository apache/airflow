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
import { Box, Link, Text } from "@chakra-ui/react";
import type { ReactNode } from "react";
import { Link as ReactRouterLink } from "react-router-dom";

import { Tooltip } from "src/components/ui/Tooltip";

import {
  formatDurationLabel,
  getTimelineItemColorPalette,
  getTimelineItemDestination,
  getTimelineItemLinkLabel,
} from "./timelineUtils";
import type { TimelineItem } from "./types";

type TimelineBarProps = {
  readonly height: string;
  readonly item: TimelineItem;
  readonly left: string;
  readonly renderTooltip: (item: TimelineItem) => ReactNode;
  readonly showDagId?: boolean;
  readonly testId: string;
  readonly top?: string;
  readonly width: number | string;
};

export const TimelineBar = ({
  height,
  item,
  left,
  renderTooltip,
  showDagId = false,
  testId,
  top,
  width,
}: TimelineBarProps) => (
  <Tooltip content={renderTooltip(item)}>
    <Link
      _hover={{ textDecoration: "none" }}
      aria-label={getTimelineItemLinkLabel(item)}
      asChild
      bg={showDagId ? "colorPalette.solid" : undefined}
      borderRadius="sm"
      color="inherit"
      colorPalette={getTimelineItemColorPalette(item)}
      data-testid={testId}
      display="block"
      height={height}
      left={left}
      minWidth={showDagId ? undefined : width}
      opacity={item.isPlanned ? 0.8 : 1}
      overflow="hidden"
      position="absolute"
      px={showDagId ? 2 : item.durationMs > 0 ? 1 : 0}
      py={showDagId ? 1 : 0}
      top={top}
      transform={showDagId ? undefined : "translateY(-50%)"}
      width={width}
      zIndex={2}
    >
      <ReactRouterLink to={getTimelineItemDestination(item)}>
        {showDagId ? (
          <Text
            color="colorPalette.contrast"
            fontSize="xs"
            fontWeight="semibold"
            overflow="hidden"
            textOverflow="ellipsis"
            whiteSpace="nowrap"
          >
            {item.label}
          </Text>
        ) : (
          <Box
            alignItems="center"
            bg="colorPalette.solid"
            borderRadius="md"
            display="flex"
            height="100%"
            justifyContent="center"
            overflow="hidden"
            width="100%"
          >
            {item.durationMs > 0 ? (
              <Text
                color="colorPalette.contrast"
                fontSize="xs"
                fontWeight="semibold"
                overflow="hidden"
                textOverflow="ellipsis"
                whiteSpace="nowrap"
              >
                {formatDurationLabel(item.durationMs)}
              </Text>
            ) : null}
          </Box>
        )}
      </ReactRouterLink>
    </Link>
  </Tooltip>
);
