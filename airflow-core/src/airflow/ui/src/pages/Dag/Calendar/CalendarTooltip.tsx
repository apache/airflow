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
import { useMemo } from "react";
import type { ReactNode } from "react";

const TOOLTIP_MIN_WIDTH = "200px";
const TOOLTIP_MAX_WIDTH = "350px";
const TOOLTIP_MAX_HEIGHT = "200px";
const TOOLTIP_PADDING = "12px";

type Props = {
  readonly content: ReactNode;
};

export const CalendarTooltip = ({ content }: Props) => {
  const tooltipStyle = useMemo(
    () => ({
      backgroundColor: "var(--chakra-colors-bg-muted)",
      border: "1px solid var(--chakra-colors-border-emphasized)",
      borderRadius: "4px",
      color: "fg",
      fontSize: "14px",
      left: "50%",
      maxHeight: TOOLTIP_MAX_HEIGHT,
      maxWidth: TOOLTIP_MAX_WIDTH,
      minWidth: TOOLTIP_MIN_WIDTH,
      opacity: 0,
      overflowY: "auto" as const,
      padding: TOOLTIP_PADDING,
      pointerEvents: "none" as const,
      position: "absolute" as const,
      top: "22px",
      transform: "translateX(-50%)",
      transition: "opacity 0.2s, visibility 0.2s",
      visibility: "hidden" as const,
      whiteSpace: "normal" as const,
      width: "auto",
      zIndex: 1000,
    }),
    [],
  );

  const arrowStyle = useMemo(
    () => ({
      borderBottom: "4px solid var(--chakra-colors-bg-muted)",
      borderLeft: "4px solid transparent",
      borderRight: "4px solid transparent",
      content: '""',
      height: 0,
      left: "50%",
      position: "absolute" as const,
      top: "-4px",
      transform: "translateX(-50%)",
      width: 0,
    }),
    [],
  );

  return (
    <div data-tooltip style={tooltipStyle}>
      <div style={arrowStyle} />
      {content}
    </div>
  );
};
