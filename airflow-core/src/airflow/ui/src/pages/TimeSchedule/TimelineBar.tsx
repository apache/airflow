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

import type { TimeScheduleItem } from "openapi/requests/types.gen";

import { Tooltip } from "src/system-components";

import { StateIcon } from "src/components/StateIcon";

import { useDurationFormat } from "src/utils";

import { WEEK_LABEL_LINE_HEIGHT_PX } from "./constants";
import {
  getTimelineDurationSeconds,
  getTimelineItemColorPalette,
  getTimelineItemDestination,
  getTimelineItemIconState,
} from "./timelineUtils";

const STATE_ICON_SIZE_PX = 10;
const WEEK_STATE_ICON_SIZE_PX = 12;

type TimelineBarProps = {
  readonly height: string;
  readonly item: TimeScheduleItem;
  readonly left: string;
  readonly renderTooltip: (item: TimeScheduleItem) => ReactNode;
  readonly testId: string;
  readonly top?: string;
  readonly width: number | string;
} & (
  | { readonly labelLineClamp: number; readonly showDagLabel: true }
  | { readonly labelLineClamp?: never; readonly showDagLabel?: false }
);

export const TimelineBar = ({
  height,
  item,
  labelLineClamp,
  left,
  renderTooltip,
  showDagLabel,
  testId,
  top,
  width,
}: TimelineBarProps) => {
  const { t: translate } = useTranslation();
  const { renderDuration } = useDurationFormat();
  const iconState = getTimelineItemIconState(item);
  const stateLabel = iconState ?? "none";
  const durationLabel = renderDuration(getTimelineDurationSeconds(item.duration_ms));

  return (
    <Tooltip content={renderTooltip(item)}>
      <Link
        _hover={{ textDecoration: "none" }}
        aria-label={`${item.dag_display_name}: ${translate(`states.${stateLabel}`)}, ${item.run_count} ${translate("dagRun", { count: item.run_count })}`}
        asChild
        bg="colorPalette.solid"
        borderRadius="sm"
        color="inherit"
        colorPalette={getTimelineItemColorPalette(item)}
        data-testid={testId}
        display="block"
        height={height}
        left={left}
        opacity={item.is_planned ? 0.8 : 1}
        overflow="hidden"
        position="absolute"
        px={showDagLabel ? 2 : 0}
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
              height={`${labelLineClamp * WEEK_LABEL_LINE_HEIGHT_PX}px`}
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
              {item.dag_display_name}
            </Text>
          ) : (
            <Box
              alignItems="center"
              bg="colorPalette.solid"
              borderRadius="md"
              color="colorPalette.contrast"
              display="flex"
              fontSize="xs"
              fontWeight="semibold"
              gap={1}
              height="100%"
              justifyContent="center"
              overflow="hidden"
              px="2px"
              width="100%"
            >
              <Box flexShrink={0} lineHeight={0}>
                <StateIcon
                  aria-hidden="true"
                  color="currentColor"
                  size={STATE_ICON_SIZE_PX}
                  state={iconState}
                />
              </Box>
              {item.duration_ms > 0 ? (
                <Text color="colorPalette.contrast" flexShrink={0} whiteSpace="nowrap">
                  {durationLabel}
                </Text>
              ) : null}
            </Box>
          )}
        </ReactRouterLink>
      </Link>
    </Tooltip>
  );
};
