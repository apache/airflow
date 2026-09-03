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
import type { BoxProps, ButtonProps } from "@chakra-ui/react";

export type CrumbShape = {
  readonly hasNotch: boolean;
  readonly hasPoint: boolean;
};

// The ">" between levels is cut by pointing each segment's end edge and notching the next one's
// start edge, then interlocking the two: they overlap by the chevron depth less the gap, so what
// shows between them is an even sliver of page background the whole way along the cut. Hovering a
// segment therefore fills its wedge too, since the wedge is part of the segment, not a gap in it.
const CHEVRON_DEPTH = 10;
const GAP = 2;
const PADDING = 3;

/** A cut on that side already separates it from its neighbor, so content can sit closer to it. */
export const CUT_PADDING = 2;
/** Keeps content clear of the wedge the chevron cuts into the segment. */
const NOTCH_PADDING = `${CHEVRON_DEPTH + 12}px`;
const POINT_PADDING = `${CHEVRON_DEPTH + 4}px`;

// clip-path is physical, so the mirrored path has to be spelled out rather than left to `dir`.
const getClipPath = ({ hasNotch, hasPoint }: CrumbShape, isRtl: boolean) => {
  const depth = `${CHEVRON_DEPTH}px`;

  if (isRtl) {
    const mirroredEnd = hasPoint ? `${depth} 0, 0 50%, ${depth} 100%` : "0 0, 0 100%";

    return `polygon(100% 0, ${mirroredEnd}, 100% 100%${hasNotch ? `, calc(100% - ${depth}) 50%` : ""})`;
  }

  const end = hasPoint
    ? `calc(100% - ${depth}) 0, 100% 50%, calc(100% - ${depth}) 100%`
    : "100% 0, 100% 100%";

  return `polygon(0 0, ${end}, 0 100%${hasNotch ? `, ${depth} 50%` : ""})`;
};

export const getSegmentStyles = (shape: CrumbShape) =>
  ({
    _rtl: { clipPath: getClipPath(shape, true) },
    bg: "bg.muted",
    clipPath: getClipPath(shape, false),
    marginInlineStart: shape.hasNotch ? `-${CHEVRON_DEPTH - GAP}px` : undefined,
  }) satisfies BoxProps;

export const getWedgePadding = (shape: CrumbShape) =>
  ({
    paddingInlineEnd: shape.hasPoint ? POINT_PADDING : PADDING,
    paddingInlineStart: shape.hasNotch ? NOTCH_PADDING : PADDING,
  }) satisfies BoxProps;

/** Column layout and the hover fill every clickable segment shares. */
export const crumbButtonStyles = {
  _hover: { bg: "bg.emphasized" },
  alignItems: "flex-start",
  borderRadius: "none",
  flexDirection: "column",
  gap: 0,
  height: "auto",
  justifyContent: "center",
  py: 1.5,
  variant: "ghost",
} satisfies ButtonProps;
