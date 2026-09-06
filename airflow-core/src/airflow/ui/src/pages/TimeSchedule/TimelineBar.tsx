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
import type { ReactNode } from "react";

import { Box, Link, Text } from "@chakra-ui/react";
import { useTranslation } from "react-i18next";
import { Link as ReactRouterLink } from "react-router-dom";

import { Tooltip } from "src/system-components";

import { StateIcon } from "src/components/StateIcon";

import { TIMELINE_TOOLTIP_CONTENT_PROPS, WEEK_LABEL_LINE_HEIGHT_PX } from "./constants";
import {
  formatDurationLabel,
  getTimelineItemColorPalette,
  getTimelineItemDestination,
  getTimelineItemIconState,
  getTimelineItemLinkLabel,
} from "./timelineUtils";
import type { TimelineItem } from "./types";

const STATE_ICON_SIZE_PX = 10;
const WEEK_STATE_ICON_SIZE_PX = 12;

type TimelineBarProps = {
  readonly height: string;
  readonly item: TimelineItem;
  readonly labelLineClamp?: number;
  readonly left: string;
  readonly renderTooltip: (item: TimelineItem) => ReactNode;
  readonly showDagLabel?: boolean;
  readonly testId: string;
  readonly top?: string;
  readonly width: number | string;
};

export const TimelineBar = ({
  height,
  item,
  labelLineClamp,
  left,
  renderTooltip,
  showDagLabel = false,
  testId,
  top,
  width,
}: TimelineBarProps) => {
  const { t: translate } = useTranslation();
  const iconState = getTimelineItemIconState(item);
  const stateLabel = iconState ?? "none";

  return (
    <Tooltip content={renderTooltip(item)} contentProps={TIMELINE_TOOLTIP_CONTENT_PROPS}>
      <Link
        _hover={{ textDecoration: "none" }}
        aria-label={`${getTimelineItemLinkLabel(item)}: ${translate(`states.${stateLabel}`)}`}
        asChild
        bg={showDagLabel ? "colorPalette.solid" : undefined}
        borderRadius="sm"
        color="inherit"
        colorPalette={getTimelineItemColorPalette(item)}
        data-testid={testId}
        display="block"
        height={height}
        left={left}
        minWidth={showDagLabel ? undefined : width}
        opacity={item.isPlanned ? 0.8 : 1}
        overflow="hidden"
        position="absolute"
        px={showDagLabel ? 2 : item.durationMs > 0 ? 2 : 0}
        py={0}
        top={top}
        transform={showDagLabel ? undefined : "translateY(-50%)"}
        width={width}
        zIndex={2}
      >
        <ReactRouterLink to={getTimelineItemDestination(item)}>
          {showDagLabel ? (
            <Text
              color="colorPalette.contrast"
              css={{ display: "-webkit-box !important" }}
              fontSize="xs"
              fontWeight="semibold"
              height={
                labelLineClamp === undefined ? "100%" : `${labelLineClamp * WEEK_LABEL_LINE_HEIGHT_PX}px`
              }
              lineHeight={`${WEEK_LABEL_LINE_HEIGHT_PX}px`}
              maxHeight="100%"
              overflow="hidden"
              style={{
                WebkitBoxOrient: "vertical",
                WebkitLineClamp: labelLineClamp,
              }}
              whiteSpace="normal"
              wordBreak="break-all"
            >
              <StateIcon
                aria-hidden="true"
                color="currentColor"
                size={WEEK_STATE_ICON_SIZE_PX}
                state={iconState}
                style={{ display: "inline", marginInlineEnd: "4px", verticalAlign: "text-bottom" }}
              />
              {item.label}
            </Text>
          ) : (
            <Box
              alignItems="center"
              bg="colorPalette.solid"
              borderRadius="md"
              color="colorPalette.contrast"
              display="flex"
              gap={1}
              height="100%"
              justifyContent="center"
              overflow="hidden"
              width="100%"
            >
              <StateIcon
                aria-hidden="true"
                color="currentColor"
                size={STATE_ICON_SIZE_PX}
                state={iconState}
              />
              {item.durationMs > 0 ? (
                <Text color="colorPalette.contrast" fontSize="xs" fontWeight="semibold" whiteSpace="nowrap">
                  {formatDurationLabel(item.durationMs)}
                </Text>
              ) : null}
            </Box>
          )}
        </ReactRouterLink>
      </Link>
    </Tooltip>
  );
};
